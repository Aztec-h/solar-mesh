import express from 'express';
import Database from 'better-sqlite3';
import axios from 'axios';
import crypto from 'crypto';

const app = express();
app.use(express.json());

// MILESTONE 3: OFFLINE FIRST
// Initialize Local Edge SQLite DB. 
// If the internet goes down, the neighborhood continues to operate on this database.
const db = new Database('local_neighborhood.db');
db.exec(`
    CREATE TABLE IF NOT EXISTS local_events (
        event_id TEXT PRIMARY KEY,
        aggregate_id INTEGER,
        event_type TEXT,
        payload TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        synced INTEGER DEFAULT 0
    );
`);

const NEIGHBORHOOD_ID = 101; // Example: "Willow Creek Sector"
const CORE_API_URL = process.env.CORE_API_URL || 'http://localhost:3000';

// Local sensor endpoint: Solar panels push data here instead of directly to the cloud
app.post('/local/energy', (req, res) => {
    const { home_id, energy_kwh, reading_type } = req.body;
    
    // We generate the UUID locally so we have idempotency when syncing!
    const eventId = crypto.randomUUID();
    const payload = JSON.stringify({ home_id, energy_kwh, reading_type });

    // 1. Store EVENT locally (Event Sourcing at the Edge)
    const insert = db.prepare(`
        INSERT INTO local_events (event_id, aggregate_id, event_type, payload) 
        VALUES (?, ?, 'EnergyReadingSubmitted', ?)
    `);
    insert.run(eventId, home_id, payload);

    console.log(`[EDGE NODE] Recorded event offline: ${eventId} | ${energy_kwh}kWh ${reading_type}`);
    res.json({ success: true, event_id: eventId, status: "stored_locally_at_edge" });
});

// ---------------------------------------------------------
// MILESTONE 3: Eventual Consistency Sync Engine (CRDT Logic)
// ---------------------------------------------------------
// Every 10 seconds, the Edge Node attempts to connect to the central cloud.
// If it connects, it pushes all unsynced immutable events.
setInterval(async () => {
    const unsyncedEvents = db.prepare('SELECT * FROM local_events WHERE synced = 0').all();
    
    if (unsyncedEvents.length === 0) return;

    console.log(`[SYNC ENGINE] Attempting to sync ${unsyncedEvents.length} events to Central Grid...`);

    try {
        const response = await axios.post(`${CORE_API_URL}/sync/events`, {
            neighborhood_id: NEIGHBORHOOD_ID,
            events: unsyncedEvents
        });

        if (response.data.success) {
            // Transaction to mark all as synced
            const markSynced = db.prepare('UPDATE local_events SET synced = 1 WHERE event_id = ?');
            const transaction = db.transaction((events: any[]) => {
                for (const ev of events) markSynced.run(ev.event_id);
            });
            transaction(unsyncedEvents);
            
            console.log(`[SYNC ENGINE] Success! Merged ${unsyncedEvents.length} events into the central grid.`);
        }
    } catch (err: any) {
        // Expected behavior in a Solarpunk grid: Connectivity is not guaranteed!
        console.log(`[SYNC ENGINE] Sync Failed (Grid Offline). Events safely stored locally. Retrying in 10s...`);
    }
}, 10000);

app.listen(4000, () => {
    console.log('Neighborhood Edge Node listening on port 4000');
});
