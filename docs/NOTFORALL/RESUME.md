# LeyLine: Resume Points & Interview Defense

## Version 1: Detailed & Descriptive (General Backend Role)
*   Architected **LeyLine**, an event-driven microgrid operating system using **Node.js, Golang, and Python**, handling real-time telemetry and peer-to-peer energy trading for simulated neighborhoods.
*   Engineered a high-concurrency order matching engine in **Golang**, consuming immutable energy events via **Redpanda (Kafka)** to process trades with sub-millisecond latency.
*   Implemented **CQRS and Event Sourcing** using **PostgreSQL**, separating write-heavy sensor data from read-heavy dashboard queries, reducing UI load times by decoupling state aggregation.
*   Developed an offline-first **Edge Node** system using **SQLite**, allowing disconnected communities to operate locally and sync state via eventual consistency/idempotent CRDT logic upon reconnection.
*   Integrated **TimescaleDB** for high-throughput time-series telemetry and **Redis Pub/Sub** to stream live trades and dynamic pricing to the frontend via **WebSockets**.

## Version 2: Short & Punchy (For highly competitive screens)
*   Built **LeyLine**, a distributed energy trading platform using **Go, Node.js, and Redpanda**.
*   Engineered an offline-first edge sync engine using **SQLite**, merging disconnected state via idempotency.
*   Implemented **CQRS & Event Sourcing** on **PostgreSQL** for immutable ledgering and fast read models.
*   Designed a high-throughput Golang matching engine handling real-time peer-to-peer energy trades.

## Version 3: Quantifiable/Metric Heavy (The "Impact" Version)
*   Architected an event-driven microgrid platform, processing simulated telemetry across **3+ microservices** with **Redpanda**, eliminating Zookeeper overhead and reducing broker footprint by **~40%**.
*   Built a **Golang** trading engine utilizing goroutines and mutexes, capable of matching thousands of simulated energy orders per second without race conditions.
*   Implemented offline-first resilience, enabling edge nodes to cache **100% of telemetry** during network partitions and idempotently sync back to the cloud upon reconnection.
*   Optimized database writes by implementing **TimescaleDB hypertables**, isolating time-series data from relational models to maintain constant insertion rates as the dataset grows.
*   Developed a Chaos Engineering script to randomly terminate containers, proving system auto-recovery and zero-data-loss guarantees via Kafka consumer group offsets.

## Version 4: DevOps & Architecture Focus
*   Designed a distributed, event-driven architecture relying on **Redpanda, Redis, and PostgreSQL** to decouple microservice domains.
*   Authored **Kubernetes** manifests for core services, incorporating **Prometheus** instrumentation via `/metrics` endpoints for real-time observability.
*   Separated read and write models (**CQRS**), offloading heavy aggregate calculations from the event store to asynchronously updated read tables.
*   Implemented a custom Chaos Monkey bash script to test Kubernetes/Docker resilience, proving system survivability against random pod failures.

## Version 5: Solarpunk / Product Impact Focus
*   Built **LeyLine**, a distributed operating system for community microgrids, focusing on resilience and green energy sharing.
*   Engineered "Solarpunk Priority Routing," a load-shedding algorithm that guarantees critical infrastructure (Hospitals) maintains power reserves during catastrophic grid failures.
*   Developed a Carbon Savings engine that aggregates community solar generation and calculates equivalent CO2 offsets in real-time.
*   Created a Simulation Engine that predicts community survivability hours utilizing an **XGBoost** machine learning forecast pipeline.

---

## 🗣️ Anticipated Resume Questions & Probable Answers

**Q1: You mention Event Sourcing. Why didn't you just update the `current_battery` column in a database?**
*Answer:* Standard CRUD mutations destroy history. In an energy exchange, we need an auditable ledger of exactly *why* a battery is at 80% (e.g., +10 generated, -5 consumed, -15 traded). Event Sourcing makes the system append-only, which is highly performant for writes and provides a perfect audit trail for financial/energy disputes.

**Q2: Why use Redpanda instead of standard Kafka?**
*Answer:* I wanted the Kafka API for durable event streaming, but Apache Kafka requires JVM tuning and managing a Zookeeper quorum (prior to KRaft). Redpanda is written in C++, operates as a single binary, and is significantly lighter and faster, making it perfect for a modern, resource-efficient microservices architecture.

**Q3: How does your offline-first edge sync actually prevent duplicate events?**
*Answer:* The edge nodes (SQLite) generate a UUID for every event *locally* before logging it. When the internet returns, the edge node POSTs its unsynced events to the cloud. The central Postgres database checks the `event_id`. If an event already exists (due to a previous dropped connection retry), it ignores it. This idempotency is the foundation of our eventual consistency model.

**Q4: Why use Golang specifically for the Trading Engine?**
*Answer:* An order book requires extreme concurrency. Node.js is single-threaded, meaning a heavy matching loop blocks the event loop. Python's GIL prevents true parallelism. Golang's goroutines allow me to ingest messages concurrently, and the `sync.Mutex` ensures that matching operations against the in-memory order book are thread-safe and blisteringly fast.

**Q5: What is the CQRS projection doing in your system?**
*Answer:* Since we use Event Sourcing, finding a house's current battery level requires aggregating hundreds of past events, which is slow. The CQRS projection listens for new events and asynchronously updates a dedicated `house_read_models` table. This allows the frontend to query state with a simple, indexed `SELECT *` without running expensive aggregations on the fly.
