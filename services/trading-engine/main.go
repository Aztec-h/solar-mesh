package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

type EnergyEvent struct {
	EventID     string  `json:"event_id"`
	HomeID      int     `json:"home_id"`
	EnergyKwh   float64 `json:"energy_kwh"`
	ReadingType string  `json:"reading_type"`
}

type Order struct {
	HomeID   int
	Role     string
	Quantity float64
	Type     string
}

type OrderBook struct {
	mu          sync.Mutex
	Buys        []Order
	Sells       []Order
	TotalSupply float64
	TotalDemand float64
	Redis       *redis.Client
}

func (ob *OrderBook) AddOrder(o Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if o.Type == "buy" {
		ob.Buys = append(ob.Buys, o)
		ob.TotalDemand += o.Quantity
		sort.Slice(ob.Buys, func(i, j int) bool {
			return rolePriority(ob.Buys[i].Role) > rolePriority(ob.Buys[j].Role)
		})
	} else {
		ob.Sells = append(ob.Sells, o)
		ob.TotalSupply += o.Quantity
	}
	
	ob.calculateAndPublishPrice()
	ob.match()
}

func rolePriority(role string) int {
	switch role {
	case "hospital":
		return 3
	case "school":
		return 2
	case "home":
		return 1
	default:
		return 0
	}
}

// MILESTONE 2: Energy Pricing Engine (Dynamic Surge Pricing)
func (ob *OrderBook) calculateAndPublishPrice() {
	basePrice := 0.50 // Base cost in credits per kWh
	currentPrice := basePrice

	if ob.TotalSupply > 0 && ob.TotalDemand > 0 {
		// If demand exceeds supply, price goes up. If supply exceeds demand, price drops.
		ratio := ob.TotalDemand / ob.TotalSupply
		currentPrice = basePrice * ratio
	} else if ob.TotalSupply > 0 && ob.TotalDemand == 0 {
		currentPrice = 0.10 // High abundance, very cheap
	} else if ob.TotalDemand > 0 && ob.TotalSupply == 0 {
		currentPrice = 2.00 // High shortage, max cap
	}

	if currentPrice > 2.0 {
		currentPrice = 2.0
	}

	priceData := map[string]interface{}{
		"price_credits_per_kwh": currentPrice,
		"timestamp":             time.Now().Format(time.RFC3339),
	}
	jsonData, _ := json.Marshal(priceData)
	
	// Publish the new price to Redis so the Node.js API can push it to WebSockets
	ob.Redis.Publish(context.Background(), "live-pricing", jsonData)
	fmt.Printf("[PRICING] Dynamic Price updated: %.2f credits/kWh (Demand: %.2f, Supply: %.2f)\n", 
		currentPrice, ob.TotalDemand, ob.TotalSupply)
}

func (ob *OrderBook) match() {
	for len(ob.Buys) > 0 && len(ob.Sells) > 0 {
		buy := &ob.Buys[0]
		sell := &ob.Sells[0]

		tradeQty := buy.Quantity
		if sell.Quantity < buy.Quantity {
			tradeQty = sell.Quantity
		}

		tradeMsg := fmt.Sprintf("House %d (Surplus) -> House %d (Deficit, Role: %s) | Qty: %.2f kWh", 
			sell.HomeID, buy.HomeID, buy.Role, tradeQty)
			
		fmt.Printf("[TRADE MATCHED] %s\n", tradeMsg)

		// MILESTONE 2: Publish trade to WebSockets via Redis
		tradeData := map[string]interface{}{
			"seller_id": sell.HomeID,
			"buyer_id":  buy.HomeID,
			"role":      buy.Role,
			"quantity":  tradeQty,
			"timestamp": time.Now().Format(time.RFC3339),
		}
		jsonData, _ := json.Marshal(tradeData)
		ob.Redis.Publish(context.Background(), "live-trades", jsonData)

		buy.Quantity -= tradeQty
		sell.Quantity -= tradeQty
		ob.TotalDemand -= tradeQty
		ob.TotalSupply -= tradeQty

		if buy.Quantity == 0 {
			ob.Buys = ob.Buys[1:]
		}
		if sell.Quantity == 0 {
			ob.Sells = ob.Sells[1:]
		}
	}
}

func main() {
	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:19092"),
		kgo.ConsumeTopics("energy-events"),
		kgo.ConsumerGroup("trading-engine-group"),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("unable to init redpanda client: %v", err)
	}
	defer cl.Close()

	// Initialize Redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	orderBook := &OrderBook{
		Redis: rdb,
	}

	fmt.Println("SolarMesh Trading Engine Started.")
	fmt.Println("Listening for energy events on Redpanda...")

	for {
		fetches := cl.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("fetch errors: %v", errs)
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var evt EnergyEvent
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				log.Printf("failed to parse event: %v", err)
				return
			}
			
			orderType := "sell"
			if evt.ReadingType == "consumed" {
				orderType = "buy"
			}

			role := "home"
			if evt.HomeID == 1 {
				role = "hospital"
			} else if evt.HomeID == 2 {
				role = "school"
			}

			orderBook.AddOrder(Order{
				HomeID:   evt.HomeID,
				Role:     role,
				Quantity: evt.EnergyKwh,
				Type:     orderType,
			})
		})
	}
}
