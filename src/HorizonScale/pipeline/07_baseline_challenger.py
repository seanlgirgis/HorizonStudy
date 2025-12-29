"""
07_baseline_challenger.py
Author: Sean L. Girgis

PURPOSE:
    The "Challenger Engine" of the HorizonScale project. This script orchestrates 
    the training and inference of a Gradient Boosted (XGBoost) regressor. It serves 
    as a high-performance competitor to the Prophet baseline, designed to handle 
    structural complexity and non-linear patterns in enterprise asset telemetry.

DESIGN PHILOSOPHY:
    - GPU Acceleration: Leverages CUDA-enabled 'hist' tree algorithms for parallel processing.
    - Feature Engineering: Maps temporal dates to ordinal 't' (trend) and 'month' (seasonality).
    - Competitive Benchmarking: Follows a standardized 32-month training and 4-month competition split.

EXIT CRITERIA:
    - Successful population of the 'challenger_results' table in DuckDB.
    - Processing verification across approximately 8,000 host/resource pairs.
    - Valid generation of Parquet persistence files for the Tournament Layer.
"""

import duckdb
import pandas as pd
import polars as pl
import xgboost as xgb
from tqdm import tqdm
from pathlib import Path

# Project-specific internal libraries
from horizonscale.lib.config import DB_PATH, MASTER_DATA_DIR, FORECAST_HORIZON
from horizonscale.lib.logging import init_root_logging, execution_timer

def run_challenger_shop():
    """
    Core Orchestration Logic:
    Manages the end-to-end lifecycle of the XGBoost modeling shop, including
    data acquisition, GPU-accelerated training loops, and database persistence.
    """
    logger = init_root_logging("challenger_xgb")
    parquet_out = MASTER_DATA_DIR / "challenger_forecast_master.parquet"
    
    # Timer block for performance auditing and bottleneck detection
    with execution_timer(logger, "CHALLENGER XGBoost GPU Processing"):
        
        # 1. DATA ACQUISITION
        # Uses DuckDB for high-speed extraction of refined telemetry into Polars.
        with duckdb.connect(str(DB_PATH)) as con:
            full_df = con.execute("SELECT ds, y, host_id, resource FROM processed_data").pl()
        
        # 2. PARTITIONING
        # Groups the global telemetry pool by unique asset identifiers for isolated modeling.
        logger.info("PREP: Partitioning data for XGBoost feature mapping...")
        groups = full_df.group_by(['host_id', 'resource'])
        
        results = []
        
        # 3. GPU MODELING ENGINE
        # Iterates through each asset, performing feature mapping and CUDA-offloaded training.
        for (keys, group_df) in tqdm(groups, total=8000, desc="GPU Modeling"):
            try:
                h_id, res = keys[0], keys[1]
                
                # Transform to Pandas for ML-specific feature engineering
                df = group_df.to_pandas()
                df['ds'] = pd.to_datetime(df['ds'])
                df['t'] = range(len(df))  # Ordinal trend component
                df['month'] = df['ds'].dt.month # Seasonal cycle component
                
                # Split logic: Standardized 32-month training window
                train_df = df[df['ds'] < "2025-08-01"].copy()
                
                # HYPERPARAMETERS: Specialized for high-throughput GPU stability
                model = xgb.XGBRegressor(
                    tree_method='hist', # Histogram-based splitting for speed
                    device='cuda',      # Direct CUDA kernel offloading
                    n_estimators=50, 
                    max_depth=5
                )
                
                # TRAIN: Fitting on trend (t) and seasonality (month)
                model.fit(train_df[['t', 'month']], train_df['y'].astype('float32'))

                # FUTURE GRID GENERATION
                # Constructs a timeline encompassing the 4-month backtest and 6-month forecast.
                future_range = pd.date_range(
                    start=train_df['ds'].max() + pd.Timedelta(days=1), 
                    periods=120 + FORECAST_HORIZON
                )
                f_df = pd.DataFrame({'ds': future_range})
                f_df['t'] = range(len(train_df), len(train_df) + len(future_range))
                f_df['month'] = f_df['ds'].dt.month
                
                # INFERENCE: Generate point predictions
                preds = model.predict(f_df[['t', 'month']])
                
                # TOURNAMENT FORMATTING
                # Aligns XGBoost output with the project's standard schema.
                res_df = pd.DataFrame({'ds': f_df['ds'], 'yhat': preds})
                
                # CI ESTIMATION: Manual interval calculation as XGBoost lacks native CIs.
                res_df['yhat_lower'], res_df['yhat_upper'] = preds * 0.9, preds * 1.1
                res_df['host_id'], res_df['resource'], res_df['model'] = h_id, res, 'XGBoost'
                
                # METADATA TAGGING: Distinguishes between backtest and forward forecast.
                res_df['data_type'] = res_df['ds'].apply(
                    lambda x: 'backtest' if x < pd.Timestamp("2025-12-01") else 'forecast'
                )
                
                results.append(res_df)
                
            except Exception as e:
                logger.error(f"STALL: Critical error on host {h_id}: {str(e)}")
                break 

        # 4. PERSISTENCE GATES
        # Ensures atomic flushes of modeling results back into the primary DuckDB store.
        if results:
            master_df = pd.concat(results)
            logger.info(f"FLUSH: Writing {len(master_df):,} projections to Parquet...")
            pl.from_pandas(master_df).write_parquet(parquet_out)
            
            if parquet_out.exists():
                logger.info(f"SUCCESS: {parquet_out.name} verified on disk.")
                
                # PATH NORMALIZATION: Ensures cross-platform compatibility for DuckDB
                db_path_str = str(parquet_out).replace('\\', '/')
                with duckdb.connect(str(DB_PATH)) as con:
                    con.execute("DROP TABLE IF EXISTS challenger_results")
                    con.execute(f"CREATE TABLE challenger_results AS SELECT * FROM read_parquet('{db_path_str}')")
            else:
                logger.error("IO FAIL: Parquet persistence not detected.")
        else:
            logger.error("CRITICAL: No projections generated. Review logs.")

if __name__ == "__main__":
    run_challenger_shop()