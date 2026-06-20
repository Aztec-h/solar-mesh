# LeyLine: Low-Level Design (LLD)

## 1. Database Schema Design (PostgreSQL)

### Table: `homes` (Entity Metadata)
| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | Unique identifier for the building |
| `name` | VARCHAR | NOT NULL | Human readable name |
| `role` | VARCHAR | NOT NULL | `hospital`, `school`, `home` (Used for Priority load shedding) |
| `battery_capacity_kwh` | DECIMAL | NOT NULL | Maximum battery storage |

### Table: `events` (The Write Model / Event Store)
| Column | Type | Constraints | Description |
|---|---|---|---|
| `event_id` | UUID | PRIMARY KEY | Ensures idempotency during sync |
| `aggregate_id` | INT | NOT NULL | Ties to `homes.id` |
| `event_type` | VARCHAR | NOT NULL | e.g., `EnergyReadingSubmitted` |
| `payload` | JSONB | NOT NULL | Contains `energy_kwh` and `reading_type` |

### Table: `energy_readings` (TimescaleDB Time-Series)
*Configured as a Hypertable partitioned by `time`.*
| Column | Type | Constraints | Description |
|---|---|---|---|
| `time` | TIMESTAMPTZ | NOT NULL | Indexed time bucket |
| `home_id` | INT | REFERENCES | Ties to `homes.id` |
| `energy_kwh`| DECIMAL | NOT NULL | Raw telemetry value |

### Table: `house_read_models` (The CQRS Read Model)
| Column | Type | Constraints | Description |
|---|---|---|---|
| `home_id` | INT | PRIMARY KEY | O(1) lookups for UI |
| `net_surplus_kwh`| DECIMAL | NOT NULL | Pre-calculated by projection worker |

---

## 2. Sequence Flows

### Flow A: Core Telemetry Ingestion
1. Sensor posts JSON to `Core API` (`/energy`).
2. API inserts raw data into `TimescaleDB`.
3. API generates JSONB event and inserts into `events` table.
4. API calls `updateCQRSReadModel()` asynchronously to recalculate `house_read_models`.
5. API serializes the event and pushes to `Redpanda` topic `energy-events`.
6. HTTP 200 OK returned to Sensor.

### Flow B: Trading Engine Matching
1. `Trading Engine` (Go) polls `Redpanda`.
2. Consumes `EnergyReadingSubmitted` event.
3. Locks Mutex: `ob.mu.Lock()`.
4. Parses role. If `buyer` and role is `hospital`, sorts array to place it at index 0.
5. Runs `match()` algorithm.
6. Calculates surge price based on global `TotalDemand / TotalSupply`.
7. Unlocks Mutex.
8. Publishes `{seller, buyer, quantity, price}` to `Redis` channel `live-trades`.

### Flow C: Offline Edge Sync
1. Internet Drops.
2. Sensor posts to `Edge Node` (SQLite).
3. Edge generates `UUID`, saves locally.
4. Internet Returns.
5. Edge Node runs `setInterval` loop, POSTs array of events to `Core API` `/sync/events`.
6. Core API begins `Postgres Transaction`.
7. API checks `SELECT 1 FROM events WHERE event_id = UUID`.
8. If not exists: Replays events into DB, Timescale, CQRS, and Redpanda.
9. Commits transaction. Edge Node marks records as `synced = 1`.
