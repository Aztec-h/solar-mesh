# SolarMesh: Pragmatic Architecture & Execution Plan
## A distributed event-driven microgrid operating system

*"A distributed event-driven microgrid operating system for community renewable energy sharing, featuring real-time energy trading, forecasting, event sourcing, and offline-first neighborhood synchronization."*

### 1. The Strategy: Pragmatism > Over-Engineering
The goal is to build a massive, portfolio-defining distributed system without getting stuck in "infrastructure hell" for 8 months. We will use industry-standard, recognizable technologies (Redis, Postgres) for the foundation, while reserving "exotic" tech (CRDTs, Redpanda) for the specific features that make this project uniquely impressive.

### 2. Core Features & "Solarpunk" Mechanics
This isn't just a green theme; the architecture enforces Solarpunk values:
*   **Priority Allocation:** The system inherently routes power to Hospitals and Schools before private homes.
*   **Dynamic Energy Pricing:** Uber-style surge pricing. When solar is abundant, energy costs 0.1 credits. During a night-time shortage, it costs 1.2 credits.
*   **Carbon Savings Engine:** Quantifiable impact. The system calculates and displays the exact kg of CO₂ saved by the community's local generation.
*   **Grid Failure Simulation:** The backend can simulate a neighborhood disconnect and calculate community survivability (e.g., "Battery lasts 17 hours").
*   **Offline-First Resilience:** Neighborhoods can run locally and sync state when internet returns.

### 3. Technology Stack (The "Resume Gold" Stack)

| Component | Selected Technology | Rationale |
| :--- | :--- | :--- |
| **API Gateway** | **Traefik** (or Nginx) | Simple, Docker-native routing. Easier to configure for V1 than Envoy. |
| **Primary DB** | **PostgreSQL** | Rock-solid for Users, Auth, and our initial Event Sourcing (Event Table). |
| **Time-Series DB** | **TimescaleDB** | Extension on top of Postgres. Keeps our infrastructure footprint small while giving us insane ingestion/aggregation speed for sensor data. |
| **Message Broker** | **Redpanda** | The event backbone. Drops in like Kafka but in C++, avoiding JVM overhead and Zookeeper. |
| **Trading Service** | **Golang** | Goroutines are perfect for a low-latency, highly concurrent in-memory order matching engine. |
| **Cache & Pub/Sub** | **Redis** | Industry standard. Interviewers immediately recognize Redis Streams and PubSub. |
| **Analytics/ML** | **Python (FastAPI)** | Native integration with ML libraries (XGBoost) for generation forecasting. |
| **Edge Sync** | **SQLite + CRDTs** | ElectricSQL/PowerSync for offline-first, eventual consistency at the neighborhood edge. |

### 4. Milestone-Driven Execution Plan

This ensures we actually ship a working product at every stage, rather than getting stuck.

#### Milestone 1: The MVP (Core Trading & Event Streams)
*Goal: Prove the core concept. Entities exist, energy flows, and trades happen.*
*   **Infra:** Docker Compose with Postgres, Redis, and Redpanda.
*   **Foundation:** Basic Auth and Postgres schema (Homes, Batteries, Roles).
*   **Event Table Sourcing:** Use Postgres as a simple event store (`events` table with `aggregate_id`, `event_type`, `payload`). Publish to Redpanda.
*   **Energy & Battery Services:** Simple services to ingest metrics and track capacity.
*   **The Trading Engine (Golang):** The centerpiece. Match surplus houses with deficit houses, enforcing the Hospital/School priority rules.

#### Milestone 2: Intelligence & Impact (The "Wow" Factor)
*Goal: Make the UI sing and the system smart.*
*   **Energy Pricing Engine:** Implement dynamic price calculations based on the global supply/demand ratio.
*   **Carbon Savings Engine:** Add aggregations to show real-world environmental impact.
*   **Forecasting (Python):** Predict tomorrow's generation and peak load hours.
*   **WebSockets:** Push live trades and price updates to the frontend via Redis Pub/Sub.

#### Milestone 3: The Solarpunk Edge (Distributed Resilience)
*Goal: The offline-first capability that blows interviewers away.*
*   **Proper CQRS:** Fully separate read models from write models.
*   **Edge Nodes:** Introduce local SQLite databases simulating neighborhood hubs.
*   **CRDT Sync:** Implement the mathematical sync engine to merge disconnected neighborhood states back into the main grid without conflicts.

#### Milestone 4: Enterprise Scale (DevOps & Simulation)
*Goal: Prove senior-level operations and testing capabilities.*
*   **Grid Failure Simulation:** Build the simulation engine to test community survivability under constraints.
*   **Kubernetes:** Move from Docker Compose to K8s manifests.
*   **Observability:** Add Prometheus and Grafana for metrics tracing.
*   **Chaos Testing:** Randomly kill services and prove the system recovers.
