# LeyLine: Project Challenges & Solutions

## Challenge 1: The Race Condition in the Order Book
**The Problem:** 
The Golang Trading Engine ingests events from Redpanda. Because Redpanda is so fast, multiple energy events (e.g., a "Sell" and a "Buy" order) would arrive simultaneously. The matching engine loops through the arrays to match them. Without protection, two goroutines would attempt to slice the array and mutate `Quantity` at the exact same time, leading to `panic: index out of range` or corrupted trade data.

**The Solution:** 
I implemented a `sync.Mutex` on the `OrderBook` struct. Every time `AddOrder` is called, the mutex is locked (`ob.mu.Lock()`), ensuring that sorting the priority queue (putting Hospitals first) and executing the `match()` logic occurs sequentially. While this introduces a tiny bottleneck, it mathematically guarantees data integrity.

## Challenge 2: Achieving Idempotency in Edge Syncing
**The Problem:**
In Milestone 3, I built Edge Nodes that operate offline. When the internet reconnects, they push their saved events to the cloud. However, if the internet flickers *during* the sync, the Node.js server might save the data but fail to send the HTTP 200 OK response. The Edge Node assumes failure and resends the same events 10 seconds later, doubling the energy generation.

**The Solution:**
I shifted from server-generated IDs to client-generated IDs. The Edge Node generates a cryptographic UUID for the event *before* saving it offline. When syncing, the Core API checks Postgres: `SELECT 1 FROM events WHERE event_id = $1`. If the UUID exists, it skips insertion. This makes the `/sync` endpoint perfectly idempotent—it can be safely retried infinitely without corrupting the ledger.

## Challenge 3: Coupling Read and Write Workloads (The CQRS Pivot)
**The Problem:**
Initially, to get a neighborhood's battery state for the UI, I had to run a `SUM()` aggregation over thousands of rows in the `events` table. As the mock telemetry grew, the UI dashboard latency spiked.

**The Solution:**
I decoupled the system using Command Query Responsibility Segregation (CQRS). I created a `house_read_models` table. The API writes to the `events` log instantly, and asynchronously triggers a projection function that pre-calculates the net surplus and caches it in the read model. The UI now does a simple indexed `SELECT *`, reducing query time from ~200ms to <2ms.

## Challenge 4: Real-time PubSub Fan-out
**The Problem:**
When deploying multiple replicas of the Node.js API (to handle HTTP load), WebSocket connections became isolated. If the Go Engine matched a trade and sent an HTTP hook to Node Instance A, only the clients connected to Instance A saw the update. Clients on Instance B were blind.

**The Solution:**
I introduced Redis as a Pub/Sub backplane. The Go engine publishes trades to a Redis channel (`live-trades`). All instances of the Node.js API subscribe to this channel. Now, no matter which container a user is connected to, the WebSocket server catches the Redis event and emits it to the frontend.
