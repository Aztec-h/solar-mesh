# LeyLine: Architecture & Technical Tradeoffs

## Architecture Overview
LeyLine is a distributed, event-driven system built around the concept of **Command Query Responsibility Segregation (CQRS)** and **Event Sourcing**. 
Instead of microservices mutating a shared state, they communicate by publishing immutable facts (Events) to a central nervous system (Redpanda).

### The Flow:
1. **Command:** A sensor submits an energy reading.
2. **Event:** The Core API validates the command and writes an `EnergyReadingSubmitted` event to Postgres.
3. **Propagation:** The event is published to Redpanda.
4. **Reaction:** The Go Trading Engine consumes the event, updates its in-memory order book, matches trades, and calculates dynamic pricing.
5. **View Update:** Background workers project the new events into a fast read-model (`house_read_models`) for the UI.
6. **Notification:** Trades and prices are published to Redis, which the Core API streams to clients via WebSockets.

---

## 🛠️ Technology Choices & Why we REJECTED Alternatives

### 1. Message Broker: Redpanda vs. Apache Kafka
*   **Why Redpanda?** It is a drop-in Kafka replacement written in C++. It bypasses the Linux page cache for direct disk I/O, uses a thread-per-core architecture, and eliminates the need for the JVM and Zookeeper.
*   **Why not Kafka?** Running Kafka locally via Docker requires significant memory overhead (JVM) and complex configuration. Redpanda proves an understanding of modern, high-performance infrastructure trends.

### 2. Event Store: PostgreSQL vs. EventStoreDB
*   **Why Postgres?** For V1, a simple `events` table in Postgres (with a JSONB payload and UUID) is sufficient, highly reliable, and keeps the operational footprint small.
*   **Why not EventStoreDB?** EventStoreDB is the gold standard for Event Sourcing, but the learning curve and operational overhead for a portfolio project are too high. It risks bogging down the project in infrastructure details rather than delivering features. (Slated for V2).

### 3. Caching & Pub/Sub: Redis vs. DragonflyDB
*   **Why Redis?** Redis is the universal standard. Interviewers immediately understand "Redis Pub/Sub for WebSockets".
*   **Why not Dragonfly?** Dragonfly is a modern, multi-threaded Redis replacement. While technically superior for vertical scaling, using Redis demonstrates foundational knowledge of industry standards. Over-optimizing the cache layer in V1 is premature.

### 4. Time-Series Data: TimescaleDB vs. QuestDB vs. InfluxDB
*   **Why TimescaleDB?** It is an extension of PostgreSQL. It allows us to keep our relational data (Users) and time-series data (Telemetry) in the same database ecosystem, reducing operational complexity while providing automatic partitioning (hypertables).
*   **Why not InfluxDB?** Influx uses a proprietary query language (Flux), which adds a learning curve.
*   **Why not QuestDB?** QuestDB is faster for pure ingestion, but Timescale's SQL compatibility makes integration with the existing Node.js ORM/driver much easier.

### 5. API Gateway: Traefik/Nginx vs. Envoy
*   **Why Traefik/Nginx?** Simple, battle-tested, and excellent Docker integration.
*   **Why not Envoy?** Envoy is built for massive service meshes (like Istio). Using it for a 3-service architecture is massive overkill and adds unnecessary YAML configuration hell.

---

## ⚖️ Deep System Tradeoffs

### Tradeoff 1: Eventual Consistency vs. Strong Consistency
*   **Decision:** We chose Eventual Consistency. When an Edge Node syncs, or when a trade is matched, the UI read-model isn't updated atomically in the same transaction.
*   **Tradeoff:** 
    *   *Pro:* Massive write throughput. The system never blocks a sensor reading waiting for the trading engine to finish.
    *   *Con:* A user might refresh their dashboard and temporarily see an outdated battery level (stale read) until the CQRS projection catches up.

### Tradeoff 2: In-Memory Order Book (Go) vs. Database Order Book
*   **Decision:** The Golang Trading Engine holds the order book in memory (using structs and arrays).
*   **Tradeoff:**
    *   *Pro:* Lightning-fast matching. No network or disk I/O bottlenecks during the matching algorithm.
    *   *Con:* If the Golang container crashes, the in-memory state is lost. 
    *   *Mitigation:* Because we use Event Sourcing, when the Go container restarts, it simply replays the Redpanda topic from its last committed offset to instantly rebuild the exact order book state in memory.

### Tradeoff 3: CRDT Sync Idempotency vs. Strict Vector Clocks
*   **Decision:** Edge nodes sync by generating a UUID offline. The server checks if the UUID exists before inserting.
*   **Tradeoff:**
    *   *Pro:* extremely simple to implement and guarantees no duplicate events.
    *   *Con:* It relies on a centralized "truth" (the Postgres DB). True mathematical CRDTs or Vector clocks would allow edge nodes to sync with *each other* peer-to-peer without the central cloud. We sacrificed true decentralized peer-to-peer syncing for architectural simplicity.
