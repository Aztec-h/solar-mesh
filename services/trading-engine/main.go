package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// EnergyEvent matches the payload sent by the Core API
type EnergyEvent struct {
	EventID     string  `json:"event_id"`
	HomeID      int     `json:"home_id"`
	EnergyKwh   float64 `json:"energy_kwh"`
	ReadingType string  `json:"reading_type"` // "generated" or "consumed"
}

// Order represents a bid or ask in the microgrid exchange
type Order struct {
	HomeID   int
	Role     string // "hospital", "school", "home"
	Quantity float64
	Type     string // "buy" (deficit), "sell" (surplus)
}

type OrderBook struct {
	mu     sync.Mutex
	Buys   []Order // Deficits (Needs energy)
	Sells  []Order // Surpluses (Has extra energy)
}

func (ob *OrderBook) AddOrder(o Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if o.Type == "buy" {
		ob.Buys = append(ob.Buys, o)
		// PRIORITY SCHEDULING RULE:
		// Hospitals first, then Schools, then Homes.
		sort.Slice(ob.Buys, func(i, j int) bool {
			return rolePriority(ob.Buys[i].Role) > rolePriority(ob.Buys[j].Role)
		})
	} else {
		ob.Sells = append(ob.Sells, o)
	}
	
	// Attempt to match orders whenever a new one is added
	ob.match()
}

// rolePriority maps a Solarpunk role to a numeric priority level
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

// match is the core trading logic
func (ob *OrderBook) match() {
	for len(ob.Buys) > 0 && len(ob.Sells) > 0 {
		buy := &ob.Buys[0]
		sell := &ob.Sells[0]

		tradeQty := buy.Quantity
		if sell.Quantity < buy.Quantity {
			tradeQty = sell.Quantity
		}

		fmt.Printf("[TRADE MATCHED] House %d (Surplus) -> House %d (Deficit, Role: %s) | Qty: %.2f kWh\n", 
			sell.HomeID, buy.HomeID, buy.Role, tradeQty)

		buy.Quantity -= tradeQty
		sell.Quantity -= tradeQty

		// Remove fulfilled orders
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

	orderBook := &OrderBook{}

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
			
			// For MVP: Simple logic. 
			// If you generated energy, you want to sell the surplus.
			// If you consumed energy, you need to buy to cover the deficit.
			// (In V2, we would calculate this based on battery levels)
			orderType := "sell"
			if evt.ReadingType == "consumed" {
				orderType = "buy"
			}

			// Mocking Role Enrichment (In reality, we'd fetch this from Redis or Postgres)
			role := "home"
			if evt.HomeID == 1 {
				role = "hospital"
			} else if evt.HomeID == 2 {
				role = "school"
			}

			fmt.Printf("[EVENT RECEIVED] Home %d | Type: %s | Qty: %.2f kWh\n", evt.HomeID, orderType, evt.EnergyKwh)

			orderBook.AddOrder(Order{
				HomeID:   evt.HomeID,
				Role:     role,
				Quantity: evt.EnergyKwh,
				Type:     orderType,
			})
		})
	}
}
