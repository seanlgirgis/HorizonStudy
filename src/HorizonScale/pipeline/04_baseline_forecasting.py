"""
04_baseline_forecasting.py
Author: Sean L. Girgis

Purpose:
    The "Forecasting Laboratory." This script selects 10 representative hosts—one 
    from each behavioral variety—and projects their utilization 180 days into 
    the future using Facebook Prophet with logistic constraints.

Genesis (Entrance Criteria):
    - 'processed_data' table must exist in DuckDB (from Stage 03).
    - 'hosts' table must be populated for behavioral scouting.

Success (Exit Criteria):
    - Forecast results (yhat, bounds) are persisted to the 'forecasts' table.
    - 10 visualization PNGs are exported to the plots/forecasts directory.
    - All projections strictly observe the [0, 100] utilization envelope.
"""

import duckdb
import pandas as pd
from prophet import Prophet
import random
import matplotlib.pyplot as plt
from pathlib import Path
from horizonscale.lib.config import (
    DB_PATH, PLOTS_DIR, FORECAST_HORIZON, Scenario, SCENARIO_VARIANTS
)
from horizonscale.lib.logging import init_root_logging

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

def check_genesis_criteria():
    """
    ENTRANCE AUDIT: Verifies the presence of the Analytical Base Table (ABT).
    """
    logger.info("--- Validating Forecasting Genesis ---")
    con = duckdb.connect(str(DB_PATH))
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'processed_data'"
    ).fetchone()[0]
    con.close()
    
    if not table_exists:
        logger.error("STOP: 'processed_data' table missing. Run 03_data_pipeline.py.")
        raise FileNotFoundError("Missing refinery output.")
    logger.info("PASS: ABT detected. Initializing Prophet engine...")

def get_diverse_modeling_targets(con: duckdb.DuckDBPyConnection):
    """
    SCOUTING: Identifies unique UUIDs for each of the 10 behavioral varieties.
    Ensures the laboratory output covers the full spectrum of enterprise DNA.
    """
    targets = []
    for scenario_enum in Scenario:
        s_search = scenario_enum.name # UPPERCASE matching DB
        
        variants = SCENARIO_VARIANTS.get(scenario_enum, {})
        for v_type in ['common', 'rare']:
            v_search = variants.get(v_type, "").lower() # lowercase matching DB
            if not v_search: continue
                
            query = f"""
                SELECT node_name, scenario, variant
                FROM hosts 
                WHERE scenario = '{s_search}' AND variant = '{v_search}'
                LIMIT 1
            """
            
            result = con.execute(query).fetchone()
            if result:
                targets.append({
                    'host_id': result[0],
                    'resource': random.choice(['cpu', 'memory']),
                    'scenario': result[1],
                    'variant': result[2]
                })
    return targets

def persist_forecast_results(host_id, resource, forecast_df):
    """
    PERSISTENCE: Stores projections in DuckDB for downstream dashboards.
    Implements a 'Clean and Insert' pattern for idempotency.
    """
    con = duckdb.connect(str(DB_PATH))
    
    # Prepare schema-compliant subset
    persist_df = forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    persist_df['host_id'] = host_id
    persist_df['resource'] = resource
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            ds TIMESTAMP, yhat DOUBLE, yhat_lower DOUBLE, yhat_upper DOUBLE,
            host_id VARCHAR, resource VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Remove stale forecasts to maintain a single source of truth
    con.execute("DELETE FROM forecasts WHERE host_id = ? AND resource = ?", [host_id, resource])
    con.execute("INSERT INTO forecasts (ds, yhat, yhat_lower, yhat_upper, host_id, resource) SELECT * FROM persist_df")
    con.close()

def run_forecasting_lab():
    """
    ORCHESTRATION: Executes the batch forecasting lifecycle.
    """
    check_genesis_criteria()
    con = duckdb.connect(str(DB_PATH))
    targets = get_diverse_modeling_targets(con)
    
    for target in targets:
        host_id, resource = target['host_id'], target['resource']
        label = f"{target['scenario'].lower()}_{target['variant'].lower()}"
        
        # Pull history from ABT
        query = f"SELECT ds, y FROM processed_data WHERE host_id = '{host_id}' AND resource = '{resource}' ORDER BY ds"
        df = con.execute(query).df()
        
        # TRAIN: Applying physical constraints [0, 100]
        df['cap'], df['floor'] = 100, 0
        model = Prophet(growth='logistic', yearly_seasonality=True, weekly_seasonality=True)
        model.fit(df)

        # PROJECT: Future time-grid generation
        future = model.make_future_dataframe(periods=FORECAST_HORIZON)
        future['cap'], future['floor'] = 100, 0
        forecast = model.predict(future)

        # PERSIST: Store for reporting
        persist_forecast_results(host_id, resource, forecast)

        # VISUALIZE: Save gallery image
        output_dir = PLOTS_DIR / "forecasts"
        output_dir.mkdir(parents=True, exist_ok=True)
        fig = model.plot(forecast)
        plt.title(f"Capacity Forecast: {host_id} ({resource}) | {label}")
        plt.savefig(output_dir / f"forecast_{label}_{resource}.png")
        plt.close(fig)
        
    con.close()
    logger.info("LAB SHUTDOWN: Projections complete.")

if __name__ == "__main__":
    run_forecasting_lab()