import express from 'express';
import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { createServer } from 'http';
import { Server } from 'socket.io';

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

// Redis clients for caching and Pub/Sub subscriptions
const redisClient = createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
const redisSubscriber = redisClient.duplicate();

// --- EXISTING ENDPOINTS ---

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

// --- NEW MILESTONE 2: CARBON SAVINGS ENGINE ---
// Assuming 1 kWh of solar generation saves roughly 0.4 kg of CO2 emissions.
const CO2_SAVED_PER_KWH = 0.4;

app.get('/impact', async (req, res) => {
    try {
        const result = await pool.query(
            "SELECT SUM(energy_kwh) as total_generated FROM energy_readings WHERE reading_type = 'generated'"
        );
        
        const totalGenerated = result.rows[0].total_generated || 0;
        const co2SavedKg = totalGenerated * CO2_SAVED_PER_KWH;
        
        res.json({
            total_solar_generated_kwh: parseFloat(totalGenerated),
            co2_emissions_saved_kg: co2SavedKg,
            equivalent_trees_planted: Math.floor(co2SavedKg / 21) // 1 tree absorbs ~21kg CO2/year
        });
    } catch (err: any) {
        res.status(500).json({ error: err.message });
    }
});

// --- NEW MILESTONE 2: WEBSOCKETS via REDIS PUB/SUB ---
io.on('connection', (socket) => {
    console.log(`[WebSockets] Client connected: ${socket.id}`);
});

async function start() {
    await producer.connect();
    console.log('Connected to Redpanda');
    
    await redisClient.connect();
    await redisSubscriber.connect();
    console.log('Connected to Redis');

    // Subscribe to live events pushed from the Golang Trading Engine
    await redisSubscriber.subscribe('live-trades', (message) => {
        // Broadcast the trade to all connected frontend clients
        io.emit('trade', JSON.parse(message));
    });

    await redisSubscriber.subscribe('live-pricing', (message) => {
        // Broadcast the new dynamic energy price
        io.emit('price_update', JSON.parse(message));
    });

    httpServer.listen(3000, () => {
        console.log('Core API & WebSockets listening on port 3000');
    });
}

start().catch(console.error);
