import axios from 'axios';

const CORE_API_URL = process.env.CORE_API_URL || 'http://localhost:3000';
const FORECAST_API_URL = process.env.FORECAST_API_URL || 'http://localhost:8000';

async function simulateGridFailure(neighborhoodId: number) {
    console.log(`\n[SIMULATION] 🚨 INITIATING CATASTROPHIC GRID FAILURE FOR NEIGHBORHOOD ${neighborhoodId} 🚨`);
    
    // 1. Fetch current battery states (from CQRS read model)
    // Normally we'd fetch all homes. We'll mock a few IDs for the demonstration.
    const homeIds = [1, 2, 3]; // 1: Hospital, 2: School, 3: Home
    let totalBatteryKwh = 0;
    
    for (const id of homeIds) {
        try {
            const res = await axios.get(`${CORE_API_URL}/state/homes/${id}`);
            const surplus = parseFloat(res.data.net_surplus_kwh);
            
            // Baseline 50kWh battery + their current net surplus
            const battery = Math.max(0, 50 + surplus); 
            totalBatteryKwh += battery;
            console.log(`[SIMULATION] House ${id} current battery level: ${battery.toFixed(2)} kWh`);
        } catch (e) {
            console.log(`[SIMULATION] House ${id} state not found. Assuming 0 battery.`);
        }
    }

    console.log(`\n[SIMULATION] Total Community Battery Reserve: ${totalBatteryKwh.toFixed(2)} kWh`);

    // 2. Fetch ML Forecast to see if tomorrow's solar will save them
    let tomorrowGen = 0;
    try {
        const forecastRes = await axios.get(`${FORECAST_API_URL}/forecast/tomorrow`);
        tomorrowGen = forecastRes.data.predicted_generation_kwh || 0;
        console.log(`[SIMULATION] Forecasted Generation for tomorrow: ${tomorrowGen.toFixed(2)} kWh`);
    } catch(e) {
        console.log(`[SIMULATION] Forecast Service Offline. Assuming 0 generation.`);
    }

    // 3. Calculate Survivability
    const averageHourlyConsumption = 15; // mock 15 kWh per hour burn rate for the neighborhood
    const hoursOfSurvival = totalBatteryKwh / averageHourlyConsumption;

    console.log(`\n--- 📊 SIMULATION RESULTS 📊 ---`);
    if (hoursOfSurvival > 24) {
        console.log(`✅ SUCCESS: The community will survive the night.`);
        console.log(`Reserve lasts for ${hoursOfSurvival.toFixed(1)} hours, well past sunrise.`);
    } else {
        const deficit = (24 * averageHourlyConsumption) - totalBatteryKwh;
        console.log(`❌ CRITICAL: The community will run out of power in ${hoursOfSurvival.toFixed(1)} hours.`);
        console.log(`❌ Action Required: Need ${deficit.toFixed(2)} kWh to survive until morning solar generation starts.`);
        
        // Priority shedding simulation (The Solarpunk rules)
        console.log(`\n🔌 Initiating Automated Load Shedding...`);
        console.log(`🔌 Civilian Home power cut. Redirecting remaining ${totalBatteryKwh.toFixed(2)} kWh strictly to Hospital (Home ID 1).`);
        
        const hospitalSurvival = totalBatteryKwh / 5; // Hospital burns 5 kWh/hour alone
        console.log(`🏥 Hospital will now remain operational for ${hospitalSurvival.toFixed(1)} hours.`);
    }
    console.log(`--------------------------------\n`);
}

// Run the simulation
simulateGridFailure(101).catch(console.error);
