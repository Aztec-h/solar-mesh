CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE homes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL, -- 'home', 'hospital', 'school'
    battery_capacity_kwh DECIMAL NOT NULL,
    current_battery_kwh DECIMAL NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Basic Event Sourcing Table
CREATE TABLE events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id INT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TimescaleDB Hypertable for Time-Series Telemetry
CREATE TABLE energy_readings (
    time TIMESTAMPTZ NOT NULL,
    home_id INT NOT NULL REFERENCES homes(id),
    energy_kwh DECIMAL NOT NULL,
    reading_type VARCHAR(20) NOT NULL -- 'generated' or 'consumed'
);

SELECT create_hypertable('energy_readings', 'time');

-- MILESTONE 3: CQRS READ MODEL
-- Separating the "Write" (events) from the "Read" (this table).
CREATE TABLE house_read_models (
    home_id INT PRIMARY KEY REFERENCES homes(id),
    total_generated_kwh DECIMAL NOT NULL DEFAULT 0,
    total_consumed_kwh DECIMAL NOT NULL DEFAULT 0,
    net_surplus_kwh DECIMAL NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
