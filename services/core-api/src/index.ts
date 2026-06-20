import express from 'express';
import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';

const app = express();
app.use(express.json());

const pool = new Pool({
    user: 'admin',
    host: process.env.DB_HOST || 'localhost',
    database: 'solarmesh',
    password: 'password',
    port: 5432,
});

const kafka = new Kafka({
    clientId: 'core-api',
    brokers: [process.env.REDPANDA_BROKER || 'localhost:19092']
});
const producer = kafka.producer();

const redisClient = createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379'
});

// Endpoint: Register a new home/hospital/school
app.post('/homes', async (req, res) => {
    const { name, role, battery_capacity_kwh } = req.body;
    try {
        const result = await pool.query(
            'INSERT INTO homes (name, role, battery_capacity_kwh) VALUES ($1, $2, $3) RETURNING *',
            [name, role, battery_capacity_kwh]
        );
        res.json(result.rows[0]);
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// Endpoint: Submit energy telemetry (generation or consumption)
app.post('/energy', async (req, res) => {
    const { home_id, energy_kwh, reading_type } = req.body; // reading_type: 'generated' or 'consumed'
    try {
        // 1. Save telemetry to TimescaleDB hypertable
        await pool.query(
            'INSERT INTO energy_readings (time, home_id, energy_kwh, reading_type) VALUES (NOW(), $1, $2, $3)',
            [home_id, energy_kwh, reading_type]
        );

        // 2. Event Sourcing: Store the event in the events table
        const eventPayload = { home_id, energy_kwh, reading_type };
        const eventResult = await pool.query(
            "INSERT INTO events (aggregate_id, event_type, payload) VALUES ($1, 'EnergyReadingSubmitted', $2) RETURNING event_id",
            [home_id, eventPayload]
        );

        // 3. Publish Event to Redpanda for the Trading Engine
        await producer.send({
            topic: 'energy-events',
            messages: [
                { key: String(home_id), value: JSON.stringify({ event_id: eventResult.rows[0].event_id, ...eventPayload }) }
            ]
        });

        res.json({ success: true, event_id: eventResult.rows[0].event_id });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

async function start() {
    await producer.connect();
    console.log('Connected to Redpanda');
    
    await redisClient.connect();
    console.log('Connected to Redis');

    app.listen(3000, () => {
        console.log('Core API Service listening on port 3000');
    });
}

start().catch(console.error);
