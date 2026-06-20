# LeyLine: Senior Interview & System Design Questions

## Part 1: Senior Developer Questions (Tech & Architecture)

**1. How does Redpanda achieve higher performance than Kafka without Zookeeper?**
*Answer:* Redpanda is written in C++ using the Seastar framework. It uses a thread-per-core architecture, bypassing the Linux page cache to manage disk I/O directly. It replaces Zookeeper by implementing the Raft consensus algorithm directly inside the broker for metadata management.

**2. Explain the issue of "Head of Line Blocking" in message queues and how you handled it.**
*Answer:* If one message fails to process, it blocks the entire partition. In Go, I mitigated this by ensuring the order book matching is synchronous but lightweight. If external I/O was needed, I would push failed messages to a Dead Letter Queue (DLQ) rather than blocking the main consumer loop.

**3. Why did you use `sync.Mutex` in Go? What happens if you forget to unlock it?**
*Answer:* I used it to protect the OrderBook arrays (`Buys` and `Sells`) from concurrent read/writes when Redpanda events are ingested concurrently. Forgetting to unlock causes a deadlock; the next goroutine requesting the lock will block infinitely, freezing the matching engine. I strictly use `defer ob.mu.Unlock()` to prevent this.

**4. How does the Node.js Event Loop handle your WebSocket broadcasts vs database writes?**
*Answer:* Node.js offloads DB writes (network I/O) to the OS asynchronously via libuv. While waiting for the DB response, the event loop is free to handle incoming WebSocket connections or broadcast Redis Pub/Sub messages. 

**5. What is a TimescaleDB Hypertable and how does it differ from a Postgres Partition?**
*Answer:* A Hypertable is an abstraction over Postgres declarative partitioning. It automatically creates chunks (partitions) based on time intervals. It differs by providing specialized time-series functions (like `time_bucket`) and handling the chunk creation dynamically behind a single virtual table, avoiding manual partition management.

**6. If your Node.js API crashes during the `/sync` endpoint execution, what happens to the edge node's data?**
*Answer:* The Edge Node wraps the HTTP call. If it doesn't receive a 200 OK, it keeps the `synced = 0` flag locally in SQLite. The `/sync` endpoint uses a Postgres `BEGIN/COMMIT` transaction. If Node crashes mid-sync, Postgres rolls back the transaction. No partial data is written, and the Edge Node simply retries later.

**7. How do you scale WebSockets horizontally if you have 3 Core API containers?**
*Answer:* By using Redis Pub/Sub as a backplane. If a client connects to Container A, and the trading engine publishes a trade, Redis broadcasts that trade to Containers A, B, and C. Container A receives it and pushes it down the WebSocket to the client.

**8. Explain the difference between `Concurrency` and `Parallelism` in the context of your Go Trading Engine.**
*Answer:* Concurrency is managing multiple tasks at once (e.g., listening to Redpanda while processing an HTTP request). Parallelism is doing them at the exact same physical time (multi-core). Go's scheduler multiplexes thousands of goroutines onto a few OS threads, achieving high concurrency, and scales them across multiple CPU cores for parallelism.

*(Self-Study Note: Expand on these concepts: JWT invalidation, Postgres connection pooling (pgbouncer), XGBoost over-fitting, Redis memory eviction policies).*

---

## Part 2: System Design & Tradeoff Questions (Architectural Defense)

**1. Let's say LeyLine goes global. Redpanda gets 100,000 events/sec. Your Go Trading engine can only process 10,000/sec. How do you scale the matching engine?**
*Answer:* Partitioning. I cannot simply spin up 10 Go containers consuming the same topic, because matching requires global state (the order book). I would partition the Redpanda topic by `neighborhood_id`. Then, I can spin up 10 Go consumers, each handling a specific subset of neighborhoods. Since neighborhoods don't share energy across the ocean, state remains localized and safely parallelized.

**2. You chose Event Sourcing. What is the "Event Store Bloat" problem, and how do you solve it in 5 years when you have 10 billion events?**
*Answer:* Replaying 10 billion events to find a battery's current state would take too long. The solution is **Snapshots**. Every 1,000 events, the system calculates the current state and saves a snapshot. To get current state, the system loads the latest snapshot and only replays the events that occurred *after* it.

**3. Your Edge Node syncs data after an internet outage. What happens if a house "traded" energy locally, but central pricing changed during the outage? How do you resolve the conflict?**
*Answer:* This is a classic CRDT/Distributed Systems conflict. 
*Tradeoff:* Do we favor local autonomy or central truth? 
*Solution:* We implement a "Logical Clock" or use the central price timestamp. If the trade was executed offline based on an old cached price, the central server accepts the trade but flags it as a "Reconciled Trade," calculating the financial delta. We prioritize the physical reality (the energy was moved) over the financial accuracy (the price was wrong), adjusting balances post-sync.

**4. Why not use Operational Transformation (OT) instead of Idempotency/CRDTs for offline sync?**
*Answer:* OT is great for collaborative text editing (like Google Docs) where order operations need transformation based on index shifts. For energy events, events are commutative (A + B = B + A). If I generate 10kWh and consume 5kWh, the order doesn't matter to the final state. CRDT-like idempotency is much simpler and perfectly suited for commutative energy events.

**5. What is the single biggest point of failure in your current architecture, and how would you fix it?**
*Answer:* The Postgres database. While Redpanda and Core API are distributed, a single Postgres instance going down halts all new writes and CQRS projections. 
*Fix:* I would implement Postgres High Availability (HA) using Patroni to manage a primary-replica cluster with automatic failover, and use PgBouncer to manage connection pooling so application connections don't break during the failover window.

**6. If your system is under heavy load, the CQRS Read Model projection starts lagging behind the Event Store. The UI shows a user has 50kWh, but they actually have 0kWh and try to trade it. How do you handle this?**
*Answer:* This is the danger of Eventual Consistency. To fix it, the Trading Engine (which processes the command) must be the source of truth, NOT the read model. When the user submits a trade, the Go engine checks its in-memory state (which is up-to-date with the event stream). It rejects the command before it ever becomes an event, emitting an `OrderRejected` event instead.

**7. How would you handle "Slow Consumers" on your WebSocket connections?**
*Answer:* If the server generates trades faster than a mobile client on 3G can receive them, memory on the Node.js server will bloat with buffered TCP packets. I would implement backpressure: if a client's buffer exceeds a threshold, the server drops non-critical packets (like live trades) and only sends critical state updates, or simply forces a disconnect and makes the client fetch via HTTP REST upon reconnection.
