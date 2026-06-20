# LeyLine

> *Everything leaves a trace.*
> *Every event becomes history.*
> *Every connection carries energy.*

**LeyLine** is a distributed event-driven microgrid operating system. It facilitates community renewable energy sharing, real-time trading, forecasting, and offline-first synchronization. Inspired by the roots of a world carrying its memories and power, LeyLine ensures that neighborhoods remain resilient, connected, and energy-efficient.

---

## ⚡ Core Capabilities

- **The Microgrid Exchange:** A high-concurrency order-matching engine built in Golang that pairs energy surpluses with deficits in real-time.
- **Solarpunk Priority Routing:** Algorithmic load shedding that guarantees critical infrastructure (Hospitals, Schools) receives power before civilian homes during shortages.
- **Offline-First Edge Nodes:** Communities can survive internet outages. Local hubs use SQLite to log telemetry and trades, merging state back into the central grid via eventual consistency (CRDT principles) when connectivity returns.
- **Dynamic Surge Pricing:** Real-time energy cost calculation based on global supply and demand metrics.
- **Event Sourcing & CQRS:** The entire system state is derived from an immutable stream of energy events, with read models separated for blazing-fast UI performance.

## 🏗️ Technology Stack

| Domain | Technology |
|---|---|
| **Core API & WebSockets** | Node.js (TypeScript), Express, Socket.io |
| **Trading Engine** | Golang (Goroutines, Mutexes) |
| **Intelligence / ML** | Python, FastAPI, XGBoost, Pandas |
| **Edge Nodes** | SQLite (`better-sqlite3`) |
| **Data & Telemetry** | PostgreSQL, TimescaleDB |
| **Event Broker** | Redpanda (C++ Kafka alternative) |
| **Caching & Pub/Sub** | Redis |
| **DevOps & Observability** | Docker, Kubernetes, Prometheus, Chaos Bash Scripts |

## 📂 System Architecture

LeyLine utilizes a modern event-driven microservices architecture:

1. **Sensors** push data to the **Core API** (or to local **Edge Nodes** if offline).
2. The Core API logs immutable facts to a **Postgres Event Store** and raw telemetry to a **TimescaleDB Hypertable**.
3. Events are published to **Redpanda**.
4. The **Golang Trading Engine** consumes Redpanda events, matches orders, recalculates dynamic pricing, and publishes trades to **Redis**.
5. The Core API listens to Redis and blasts updates to the frontend via **WebSockets**.
6. Background workers maintain **CQRS Read Models** for instant state retrieval.

## 🚀 Deployment

The project is fully containerized. A Chaos Monkey script is included to test system resilience by randomly terminating critical containers.