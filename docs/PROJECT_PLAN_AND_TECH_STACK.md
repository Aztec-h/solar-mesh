# SolarMesh: Distributed Community Microgrid Management Platform
## Architecture & Implementation Strategy

### 1. Executive Summary
SolarMesh is a modern, distributed backend system designed to manage a solarpunk-inspired community microgrid. It handles real-time energy generation and consumption telemetry, battery state management, peer-to-peer energy trading, and predictive forecasting. The system is designed with high fault tolerance, event-driven principles, and a unique offline-first capability for neighborhood resilience.

### 2. Technology Stack & Architectural Decisions

To demonstrate deep system design knowledge and stand out from typical student projects, we are selecting technologies that solve specific distributed systems problems at scale, rather than just picking the defaults.

#### Criteria for Technology Selection
1. **High Ingestion Rate:** Energy sensors emit data constantly; the database must handle high write throughput.
2. **Concurrency & Low Latency:** The trading service requires a matching engine that can process trades instantly without race conditions.
3. **Event Sourcing & Auditability:** Financial/Energy transactions must be append-only and fully reconstructible.
4. **Resilience & Edge-Computing:** Neighborhoods must survive internet outages (The Solarpunk constraint).
5. **Modern Industry Standards:** Technologies that show an understanding of current enterprise trends (e.g., replacing Zookeeper, multi-threaded caching).

#### The Stack

| Component | Selected Technology | Why This? (System Design Rationale) | What others normally pick | Why not the alternative? |
| :--- | :--- | :--- | :--- | :--- |
| **API Gateway / Proxy** | **Envoy** (or Apache APISIX) | Designed for cloud-native microservices. Provides advanced load balancing, circuit breaking, and observability out of the box. | Nginx or Express Gateway | Nginx is harder to configure dynamically for ephemeral microservices. Envoy is the industry standard for service meshes. |
| **Core Relational DB** | **PostgreSQL** | ACID compliance for user accounts, RBAC, and static configuration (house roles, locations). | MongoDB | Relational data with strict schemas is crucial for billing and roles. NoSQL is a poor fit for structured financial/energy ledgers. |
| **Time-Series DB** | **QuestDB** or **TimescaleDB** | Optimized for high-throughput time-series data (sensor readings). QuestDB uses columnar storage and is insanely fast for aggregations. | Standard Postgres or InfluxDB | Standard PG bloats with high-frequency telemetry. InfluxDB has a custom query language (Flux/InfluxQL), whereas QuestDB/Timescale uses standard SQL, making integration easier. |
| **Message Broker** | **Redpanda** | A drop-in replacement for Kafka written in C++. No JVM, no Zookeeper. 10x faster and much lighter on resources. | Apache Kafka or RabbitMQ | Kafka requires heavy infrastructure and JVM tuning. RabbitMQ is push-based and not designed for durable event streaming and replay (which we need for Event Sourcing). |
| **Trading Service** | **Golang** | Goroutines and channels make it trivial to build a highly concurrent, low-latency in-memory order matching engine. | Node.js or Python | Node is single-threaded (event loop blocking on heavy math). Python is too slow for a real-time financial matching engine due to the GIL. |
| **Event Store (CQRS)** | **EventStoreDB** | Purpose-built database for Event Sourcing. Stores immutable events natively and projects state automatically. | Storing JSON in Postgres | Hacking event sourcing into a relational DB leads to complex locking and slow aggregate read times as the event log grows. |
| **Caching / State** | **DragonflyDB** | A multi-threaded, lock-free replacement for Redis. Achieves massively higher throughput on a single node. | Redis | Redis is single-threaded. Dragonfly scales vertically much better and shows you know the cutting-edge ecosystem. |
| **Forecast / ML** | **Python (FastAPI)** | Native integration with XGBoost/LightGBM. FastAPI provides fast, async endpoints for serving model inferences. | Flask or Django | Django is too heavy for a single-purpose microservice. Flask lacks native async support and automatic OpenAPI docs. |
| **Offline Sync (Edge)** | **ElectricSQL / PowerSync (CRDTs)** | Provides active-active replication between edge SQLite databases and central Postgres using Conflict-Free Replicated Data Types. | Polling / Custom Sync APIs | Custom sync logic almost always leads to split-brain scenarios and data loss. CRDTs guarantee eventual consistency mathematically. |

---

### 3. Phased Implementation Plan

Building this all at once is impossible. We will use a progressive enhancement strategy.

#### Phase 1: The Foundation (Core CRUD & Auth)
*Goal: Get the basic infrastructure running and secure.*
* **Setup Docker Compose:** Define Postgres, Envoy, and the base networking.
* **Identity Service:** Implement JWT-based Auth and RBAC (Admin, Neighborhood Manager, Resident).
* **Metadata Service:** CRUD operations for Houses, Batteries, and Roles (Hospital, School, Home).
* **Result:** A secure foundation where entities exist but don't do anything yet.

#### Phase 2: Telemetry & Time-Series (The Energy Service)
*Goal: Handle the massive influx of sensor data.*
* **Database Spin-up:** Introduce QuestDB/TimescaleDB.
* **Energy Service (Node/Go):** Create endpoints to ingest `+` and `-` kWh readings.
* **Aggregation Jobs:** Write SQL roll-ups to calculate hourly/daily consumption per house.
* **Result:** We can track who is generating and consuming power in real-time.

#### Phase 3: Event-Driven State (The Battery & Alert Services)
*Goal: Move from CRUD to Event Sourcing.*
* **Message Broker:** Spin up Redpanda.
* **Publish/Subscribe:** Energy Service publishes `EnergyConsumed` and `EnergyGenerated` events to Redpanda.
* **Battery Service:** Subscribes to events, updates internal state (battery level). If battery < 20%, it fires a `BatteryLow` event.
* **Alert Service:** Subscribes to `BatteryLow` and sends a mock notification.
* **Result:** Services communicate asynchronously. State is derived from a stream of facts.

#### Phase 4: The Solarpunk Exchange (The Trading Service)
*Goal: The algorithmic core of the platform.*
* **Matching Engine (Golang):** Implement an order book. Houses with excess energy place "Sell" orders (at 0 cost, prioritizing community). Houses with deficits place "Buy" orders.
* **Priority Rules:** Implement the logic where Hospitals and Schools automatically jump to the front of the queue for energy allocation.
* **Result:** A functioning mini-stock exchange for energy, demonstrating complex data structures and concurrency.

#### Phase 5: Intelligence (The Forecast Service)
*Goal: Make the grid smart.*
* **ML Microservice (Python):** Fetch historical data from QuestDB.
* **Modeling:** Train a simple XGBoost model to predict tomorrow's generation based on mock weather data.
* **Endpoints:** Expose `/forecast/tomorrow` for the frontend.
* **Result:** The system anticipates needs rather than just reacting to them.

#### Phase 6: The Stretch Goal (Offline-First Edge Nodes)
*Goal: Distributed resilience.*
* **Edge DB:** Setup local SQLite instances representing a neighborhood hub.
* **CRDT Integration:** Use PowerSync/ElectricSQL to define sync rules.
* **Simulation:** Disconnect a neighborhood from the main network, run trades locally, reconnect, and watch the state merge seamlessly without conflicts.
* **Result:** A truly distributed, fault-tolerant network demonstrating elite system design.

### 4. Next Steps
1. **Review and Refine:** Ensure you are comfortable with this architecture.
2. **Repository Setup:** We will initialize a monorepo (or multi-repo if preferred) structure.
3. **Phase 1 Execution:** We will begin by drafting the Docker Compose file and the initial Database schemas for the Identity and Metadata services.
