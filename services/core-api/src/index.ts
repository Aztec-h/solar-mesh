import express from 'express';
import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { createServer } from 'http';
import { Server } from 'socket.io';
import promClient from 'prom-client';

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, { cors: { origin: '*' } });

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

const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
const redisSubscriber = redisClient.duplicate();

// ---------------------------------------------------------
// MILESTONE 4: OBSERVABILITY (Prometheus Metrics)
// ---------------------------------------------------------
const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics({ register: promClient.register });

const energyEventsCounter = new promClient.Counter({
  name: 'solarmesh_energy_events_total',
  help: 'Total number of energy events ingested'
});

app.get('/metrics', async (req, res) => {
    res.set('Content-Type', promClient.register.contentType);
    res.send(await promClient.register.metrics());
});

// ---------------------------------------------------------
// MILESTONE 3: CQRS PROJECTION FUNCTION
// ---------------------------------------------------------
// This asynchronously rebuilds the read-model state from the Event Stream.
// In a true enterprise system, this listens to Redpanda, but for now we call it directly.
async function updateCQRSReadModel(home_id: number) {
    try {
        const result = await pool.query(
            `SELECT payload->>'reading_type' as type, SUM(CAST(payload->>'energy_kwh' AS DECIMAL)) as total 
             FROM events 
             WHERE aggregate_id = $1 AND event_type = 'EnergyReadingSubmitted'
             GROUP BY payload->>'reading_type'`,
            [home_id]
        );
        
        let gen = 0, con = 0;
        result.rows.forEach(r => {
            if (r.type === 'generated') gen = parseFloat(r.total);
            if (r.type === 'consumed') con = parseFloat(r.total);
        });
        const net = gen - con;

        // Upsert the aggregated data into the Read Model table
        await pool.query(
            `INSERT INTO house_read_models (home_id, total_generated_kwh, total_consumed_kwh, net_surplus_kwh, last_updated)
             VALUES ($1, $2, $3, $4, NOW())
             ON CONFLICT (home_id) DO UPDATE SET 
                total_generated_kwh = EXCLUDED.total_generated_kwh,
                total_consumed_kwh = EXCLUDED.total_consumed_kwh,
                net_surplus_kwh = EXCLUDED.net_surplus_kwh,
                last_updated = NOW()`,
            [home_id, gen, con, net]
        );
    } catch (err) {
        console.error("CQRS Projection Error:", err);
    }
}

// --- STANDARD ENDPOINTS ---

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

app.post('/energy', async (req, res) => {
    const { home_id, energy_kwh, reading_type } = req.body;
    try {
        await pool.query(
            'INSERT INTO energy_readings (time, home_id, energy_kwh, reading_type) VALUES (NOW(), $1, $2, $3)',
            [home_id, energy_kwh, reading_type]
        );

        const eventPayload = { home_id, energy_kwh, reading_type };
        const eventResult = await pool.query(
            "INSERT INTO events (aggregate_id, event_type, payload) VALUES ($1, 'EnergyReadingSubmitted', $2) RETURNING event_id",
            [home_id, eventPayload]
        );

        // Fire projection update
        updateCQRSReadModel(home_id);

        // MILESTONE 4: Increment Prometheus Counter
        energyEventsCounter.inc();

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

// ---------------------------------------------------------
// MILESTONE 3: OFFLINE-FIRST EDGE SYNC ENDPOINT (CRDT LOGIC)
// ---------------------------------------------------------
app.post('/sync/events', async (req, res) => {
    const { neighborhood_id, events } = req.body;
    console.log(`[SYNC] Receiving ${events.length} offline events from Edge Node (Neighborhood: ${neighborhood_id})`);
    
    try {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');
            
            for (const ev of events) {
                const payload = JSON.parse(ev.payload);
                
                // Idempotency Check (Core principle for CRDT Eventual Consistency)
                // If the internet drops during sync, the edge node might retry. We avoid double-counting!
                const exists = await client.query('SELECT 1 FROM events WHERE event_id = $1', [ev.event_id]);
                if (exists.rowCount === 0) {
                    
                    // 1. Replay Event into main database
                    await client.query(
                        "INSERT INTO events (event_id, aggregate_id, event_type, payload, created_at) VALUES ($1, $2, $3, $4, $5)",
                        [ev.event_id, ev.aggregate_id, ev.event_type, payload, ev.timestamp]
                    );
                    
                    // 2. Add to TimescaleDB
                    await client.query(
                        'INSERT INTO energy_readings (time, home_id, energy_kwh, reading_type) VALUES ($1, $2, $3, $4)',
                        [ev.timestamp, payload.home_id, payload.energy_kwh, payload.reading_type]
                    );

                    // 3. Update the CQRS Read Model immediately
                    await updateCQRSReadModel(payload.home_id);

                    // 4. Send to Kafka so the Trading Engine matches orders retroactively!
                    await producer.send({
                        topic: 'energy-events',
                        messages: [{ key: String(payload.home_id), value: JSON.stringify({ event_id: ev.event_id, ...payload }) }]
                    });
                }
            }
            await client.query('COMMIT');
            res.json({ success: true, synced_count: events.length });
        } catch (e) {
            await client.query('ROLLBACK');
            throw e;
        } finally {
            client.release();
        }
    } catch (err: any) {
        console.error("Sync Error:", err);
        res.status(500).json({ error: err.message });
    }
});

// --- MILESTONE 3: CQRS READ ENDPOINT ---
// Blazing fast endpoint because we separated Read from Write.
app.get('/state/homes/:id', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM house_read_models WHERE home_id = $1', [req.params.id]);
        if (result.rowCount === 0) return res.status(404).json({ error: "No read model found." });
        res.json(result.rows[0]);
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

const CO2_SAVED_PER_KWH = 0.4;
app.get('/impact', async (req, res) => {
    try {
        const result = await pool.query("SELECT SUM(energy_kwh) as total_generated FROM energy_readings WHERE reading_type = 'generated'");
        const totalGenerated = result.rows[0].total_generated || 0;
        const co2SavedKg = totalGenerated * CO2_SAVED_PER_KWH;
        
        res.json({
            total_solar_generated_kwh: parseFloat(totalGenerated),
            co2_emissions_saved_kg: co2SavedKg,
            equivalent_trees_planted: Math.floor(co2SavedKg / 21)
        });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

io.on('connection', (socket) => {
    console.log(`[WebSockets] Client connected: ${socket.id}`);
});

async function start() {
    await producer.connect();
    console.log('Connected to Redpanda');
    
    await redisClient.connect();
    await redisSubscriber.connect();
    console.log('Connected to Redis');

    await redisSubscriber.subscribe('live-trades', (message) => {
        io.emit('trade', JSON.parse(message));
    });

    await redisSubscriber.subscribe('live-pricing', (message) => {
        io.emit('price_update', JSON.parse(message));
    });

    httpServer.listen(3000, () => {
        console.log('Core API & WebSockets listening on port 3000');
    });
}

start().catch(console.error);
