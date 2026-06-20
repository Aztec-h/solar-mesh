from fastapi import FastAPI
import pandas as pd
import xgboost as xgb
import psycopg2
import os
import datetime

app = FastAPI(
    title="SolarMesh Forecast Service",
    description="Machine Learning service for predicting community microgrid generation."
)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = "admin"
DB_PASS = "password"
DB_NAME = "solarmesh"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
        port=5432
    )

@app.get("/forecast/tomorrow")
def forecast_tomorrow():
    """
    Predicts community solar generation for tomorrow.
    Uses an XGBoost Regressor trained on TimescaleDB historical data.
    """
    conn = get_db_connection()
    query = """
        SELECT time_bucket('1 hour', time) AS hour, 
               SUM(energy_kwh) as total_gen
        FROM energy_readings 
        WHERE reading_type = 'generated' 
          AND time > NOW() - INTERVAL '7 days'
        GROUP BY hour 
        ORDER BY hour ASC;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Fallback if the database doesn't have enough telemetry yet
    if len(df) < 24:
        return {
            "status": "insufficient_data",
            "message": "Not enough historical telemetry. Returning mock XGBoost prediction.",
            "predicted_generation_kwh": 450.5,
            "peak_hour": "13:00",
            "equivalent_co2_saved_kg": 450.5 * 0.4 
        }

    # Feature Engineering
    df['hour_of_day'] = df['hour'].dt.hour
    df['day_of_week'] = df['hour'].dt.dayofweek
    
    X = df[['hour_of_day', 'day_of_week']]
    y = df['total_gen']

    # Train Regressor (In a real scenario, this is pre-trained via a pipeline)
    model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=50)
    model.fit(X, y)

    # Predict for the next 24 hours
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    tomorrow_hours = pd.DataFrame({
        'hour_of_day': range(24),
        'day_of_week': [tomorrow.weekday()] * 24
    })
    
    predictions = model.predict(tomorrow_hours)
    
    # Ensure no negative generation predictions
    predictions = [max(0, float(p)) for p in predictions]
    
    total_predicted = sum(predictions)
    peak_hour_idx = predictions.index(max(predictions))
    
    return {
        "status": "success",
        "predicted_generation_kwh": round(total_predicted, 2),
        "peak_hour": f"{peak_hour_idx:02d}:00",
        "hourly_breakdown": predictions
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
