"""
06_baseline_prophet.py
Author: Sean L Girgis

MISSION: THE CHAMPION ENGINE
This script runs the primary Prophet model across the entire enterprise fleet.
It implements a 32-month Training vs 4-month Backtest split to establish 
historical accuracy (MAPE) before projecting 180 days into the future.

GENESIS CRITERIA:
    - 'processed_data' table must exist in DuckDB.
    - Multiprocessing cores must be available for parallel execution.

EXIT CRITERIA:
    - 'prophet_results' table populated in DuckDB with ~1.4M rows.
    - 'prophet_forecast_master.parquet' written to disk for dashboarding.
    - Successful backtest metrics saved for the Script 08 Competition.
"""
"""
06_baseline_prophet.py
Author: Sean L Girgis

PRODUCTION ENGINE: Prophet High-Concurrency Shop
Features: tqdm Progress Bar, Silenced Console, Dual Persistence, Performance Timer.
"""
"""
06_baseline_prophet.py
Author: Sean L Girgis

STRATEGY: BULK IN-MEMORY FETCH
Optimized for: 64GB RAM + 20-Thread i7-12700F
"""

import duckdb
import pandas as pd
import polars as pl
from prophet import Prophet
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from horizonscale.lib.config import DB_PATH, MASTER_DATA_DIR, FORECAST_HORIZON
from horizonscale.lib.logging import init_root_logging, execution_timer

# Silence noise
import logging
logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

logger = init_root_logging(Path(__file__).stem)

# --- CONFIG ---
TRAIN_SPLIT_DATE = "2025-08-01" 
PARQUET_OUTPUT = MASTER_DATA_DIR / "prophet_forecast_master.parquet"

def encapsulated_prophet_engine(series_data):
    """Worker unit receiving pre-loaded data from RAM."""
    host_id = series_data['host_id']
    resource = series_data['resource']
    df = series_data['data']
    
    try:
        # Split history: 32mo Training
        train_df = df[df['ds'] < TRAIN_SPLIT_DATE].copy()
        train_df['cap'], train_df['floor'] = 100, 0
        
        m = Prophet(growth='logistic', yearly_seasonality=True, weekly_seasonality=True)
        m.fit(train_df)

        future = m.make_future_dataframe(periods=120 + FORECAST_HORIZON)
        future['cap'], future['floor'] = 100, 0
        forecast = m.predict(future)
        
        res = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        res['host_id'], res['resource'], res['model'] = host_id, resource, 'Prophet'
        res['data_type'] = res['ds'].apply(
            lambda x: 'backtest' if x.strftime('%Y-%m-%d') < "2025-12-01" else 'forecast'
        )
        return res
    except Exception:
        return None

def run_prophet_shop():
    with execution_timer(logger, "Bulk-Load Prophet Shop"):
        # 1. GENESIS CLEANING
        if PARQUET_OUTPUT.exists(): PARQUET_OUTPUT.unlink()
        
        # 2. THE BIG FETCH (All 8,000 series into RAM at once)
        logger.info("Performing Bulk Load: Pulling all telemetry into RAM...")
        with duckdb.connect(str(DB_PATH)) as con:
            # We use Polars to fetch everything in one shot
            full_fleet_df = con.execute("SELECT ds, y, host_id, resource FROM processed_data").pl()
        
        # Group data in memory so each task gets its own slice
        logger.info("Slicing telemetry for threading...")
        tasks = []
        for (host_id, resource), group in full_fleet_df.group_by(['host_id', 'resource']):
            # Convert slice to Pandas only for Prophet compatibility
            tasks.append({
                'host_id': host_id,
                'resource': resource,
                'data': group.to_pandas() 
            })

        # 3. THREADING: Max out your 20 logical threads
        logger.info(f"Launching 20 threads for {len(tasks)} in-memory series...")
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(tqdm(
                executor.map(encapsulated_prophet_engine, tasks), 
                total=len(tasks), 
                desc="🚀 Prophet Shop (Bulk RAM Mode)",
                unit="series"
            ))
            
        # 4. CONSOLIDATION & REFLECTION
        final_dfs = [r for r in results if r is not None]
        if final_dfs:
            master_df = pd.concat(final_dfs)
            pl.from_pandas(master_df).write_parquet(PARQUET_OUTPUT)
            
            with duckdb.connect(str(DB_PATH)) as con:
                con.execute(f"CREATE TABLE prophet_results AS SELECT * FROM read_parquet('{PARQUET_OUTPUT}')")
            
            logger.info(f"SUCCESS: {len(master_df):,} rows persisted.")

if __name__ == "__main__":
    run_prophet_shop()