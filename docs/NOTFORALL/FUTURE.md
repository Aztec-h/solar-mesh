# LeyLine: Future Roadmap & Advanced Architecture

The future of LeyLine is divided into two parts: standard technical maturity (Part 1) and cutting-edge, niche distributed systems engineering (Part 2).

---

## Part 1: The "Technical Debt & Maturation" Phase
*These are features we purposefully delayed to ensure the MVP shipped successfully.*

### 1. True EventStoreDB Integration
*   **Why:** Currently, we use Postgres to simulate an Event Store. As the event log grows to billions of rows, relational DBs struggle with append-only stream querying.
*   **Future State:** Migrate the `events` table to **EventStoreDB**. This provides native immutable streams, built-in projection engines, and gRPC subscriptions, removing the need for our custom Node.js projection logic.

### 2. Full ElectricSQL / PowerSync Integration
*   **Why:** Our current offline edge node is a mock script using basic UUID idempotency.
*   **Future State:** Deploy ElectricSQL. This sits between Postgres and the edge SQLite databases, providing mathematically proven active-active replication using true CRDTs (Conflict-Free Replicated Data Types). It handles complex relational syncing automatically.

### 3. Service Mesh (Envoy/Istio)
*   **Why:** Currently, services communicate directly via hostnames. There is no zero-trust security or advanced traffic routing.
*   **Future State:** Wrap every container in an Envoy sidecar proxy. Implement Istio to manage mTLS authentication between the Go engine and the Node API, and enable canary deployments.

---

## Part 2: The "Niche & Bleeding Edge" Phase
*These are advanced, rarely-seen features that demonstrate mastery of modern systems.*

### 1. eBPF (Extended Berkeley Packet Filter) for Zero-Overhead Observability
*   **The Concept:** Traditional observability (like our Prometheus `/metrics` endpoint) requires instrumenting code. eBPF allows us to run sandboxed programs inside the Linux kernel.
*   **Implementation:** Use tools like Cilium or Pixie to trace network calls, HTTP latencies, and Redpanda message throughput directly at the kernel level, providing deep observability with zero code changes in our Node/Go apps.
*   **HLD:** Linux Kernel -> eBPF Probes -> Cilium Agent -> Prometheus -> Grafana.

### 2. WebAssembly (Wasm) Edge Plugins for Dynamic Pricing
*   **The Concept:** Currently, the pricing algorithm is hardcoded in Go. If a neighborhood wants a custom pricing model, we have to recompile the trading engine.
*   **Implementation:** Embed a Wasm runtime (like Wasmtime) inside the Go Trading Engine. Neighborhood admins can upload custom pricing algorithms compiled to Wasm. Go executes these untrusted binaries securely in microseconds during the matching loop.
*   **HLD:** Admin UI -> Upload Wasm -> Postgres Blob Store -> Go Engine Fetches Wasm -> Wasmtime executes pricing logic.

### 3. Multi-Region Active-Active with CockroachDB
*   **The Concept:** If LeyLine scales to multiple continents, a single Postgres instance in us-east-1 causes latency for Europe and fails entirely if AWS goes down.
*   **Implementation:** Replace Postgres with CockroachDB. It speaks the Postgres wire protocol but distributes data across physical regions automatically using the Raft consensus algorithm, ensuring survival even if an entire geographic region goes offline.

### 4. DragonflyDB Cluster for Extreme Memory Scaling
*   **The Concept:** Redis is single-threaded. If the Pub/Sub trade volume exceeds a single CPU core's capacity, Redis bottlenecks the system.
*   **Implementation:** Swap Redis for DragonflyDB. It is a drop-in replacement that utilizes shared-nothing architecture and multiple threads. It can process millions of ops/sec on a single instance, vastly simplifying scaling architecture compared to a Redis Cluster setup.
