"""
04_baseline_forecasting.py
Author: Sean L Girgis

MY BASELINE FORECASTING ENGINE:
Since my servers use unique UUID names (server-xxxxxxxx), I cannot use 
hardcoded IDs. This script dynamically probes the refinery, picks a random 
target node, and runs a Facebook Prophet model to project its future load.

My mission here:
1.  SCOUT: Query the database to find real server UUIDs representing each behavioral variety.
2.  ISOLATE: Extract the 'ds' and 'y' history for each specific target.
3.  CONSTRAIN: Set a 'floor' at 0 and 'cap' at 100 to keep the math realistic.
4.  TRAIN: Feed data into Prophet using 'logistic' growth to respect those walls.
5.  PROJECT: Generate a 180-day 'Future Grid' to predict potential breaches.
"""
"""
04_baseline_forecasting.py
Author: Sean L Girgis

Standardized to handle case-insensitive scouting to ensure all 10 varieties are found.
"""
"""
04_baseline_forecasting.py
Author: Sean L Girgis
"""
"""
04_baseline_forecasting.py
Author: Sean L Girgis

MY BASELINE FORECASTING ENGINE:
Standardized to handle project-wide case mismatches by forcing lowercase 
comparisons during target acquisition.
"""
"""
04_baseline_forecasting.py
Author: Sean L Girgis

THE LABORATORY:
Picks 10 random targets (one per behavioral variety) and generates
180-day forecasts using Facebook Prophet.
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
    """Verifies that the refinery has produced the necessary tables."""
    logger.info("--- Validating Forecasting Genesis (Entrance) ---")
    con = duckdb.connect(str(DB_PATH))
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'processed_data'"
    ).fetchone()[0]
    con.close()
    
    if not table_exists:
        logger.error("STOP: 'processed_data' not found. Run 03_data_pipeline first.")
        raise FileNotFoundError("Missing processed_data table.")
    logger.info("PASS: Refined data detected. Proceeding to target acquisition.")

def get_diverse_modeling_targets(con: duckdb.DuckDBPyConnection):
    """
    SCOUTING MISSION: 
    Finds one random host for each of the 10 behavioral varieties.
    Directly matches the DB Ground Truth: UPPERCASE Scenario | lowercase variant.
    """
    targets = []
    # Iterate through the 5 Scenarios in config.py
    for scenario_enum in Scenario:
        # Match DB: Scenario column is UPPERCASE
        s_search = scenario_enum.name 
        
        variants = SCENARIO_VARIANTS.get(scenario_enum, {})
        for v_type in ['common', 'rare']:
            # Match DB: Variant column is lowercase
            v_search = variants.get(v_type, "").lower()
            if not v_search: continue
                
            # Exact match query based on the verified image data
            query = f"""
                SELECT node_name, scenario, variant
                FROM hosts 
                WHERE scenario = '{s_search}' 
                  AND variant = '{v_search}'
                LIMIT 1
            """
            
            result = con.execute(query).fetchone()
            if result:
                targets.append({
                    'host_id': result[0],
                    'resource': random.choice(['cpu', 'memory', 'disk', 'network']),
                    'scenario': result[1],
                    'variant': result[2]
                })
            else:
                logger.warning(f"SCOUT FAIL: No host found for {s_search} | {v_search}")

    return targets

def run_forecasting_lab():
    """Batch processes the 10-variety forecasting gallery."""
    check_genesis_criteria()
    con = duckdb.connect(str(DB_PATH))
    
    # 1. SCOUT: Get 10 diverse targets
    targets = get_diverse_modeling_targets(con)
    
    if not targets:
        logger.error("No targets found. Pipeline cannot continue.")
        con.close()
        return

    logger.info(f"Acquired {len(targets)} targets. Starting Prophet engine...")

    # Success tracking
    success_count = 0

    for target in targets:
        host_id, resource = target['host_id'], target['resource']
        label = f"{target['scenario'].lower()}_{target['variant'].lower()}"
        
        logger.info(f"Targeting: {host_id} | Resource: {resource} | Scenario: {label}")

        # 2. ISOLATE: Pull history from the Refinery
        query = f"""
            SELECT ds, y 
            FROM processed_data 
            WHERE host_id = '{host_id}' AND resource = '{resource}' 
            ORDER BY ds
        """
        df = con.execute(query).df()
        
        if df.empty:
            logger.warning(f"No data for {host_id} ({resource}). Skipping.")
            continue

        # 3. TRAIN: Fit the model with Logistic Growth constraints
        df['cap'], df['floor'] = 100, 0
        model = Prophet(growth='logistic', yearly_seasonality=True, weekly_seasonality=True)
        model.fit(df)

        # 4. PROJECT: Predict future breaches
        future = model.make_future_dataframe(periods=FORECAST_HORIZON)
        future['cap'], future['floor'] = 100, 0
        forecast = model.predict(future)

        # 5. VISUALIZE: Export to the plot gallery
        output_dir = PLOTS_DIR / "forecasts"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        fig = model.plot(forecast)
        plt.title(f"Forecast: {host_id} ({resource}) | {label}", pad=20)
        plt.ylim(-5, 105)
        plt.tight_layout()
        plt.subplots_adjust(top=0.9)
        
        plot_path = output_dir / f"forecast_{label}_{resource}.png"
        plt.savefig(plot_path, bbox_inches='tight')
        plt.close(fig)
        
        logger.info(f"SUCCESS: Saved forecast gallery image to {plot_path.name}")
        success_count += 1

    con.close()
    logger.info(f"Forecasting complete. Generated {success_count} / 10 variety plots.")

if __name__ == "__main__":
    run_forecasting_lab()