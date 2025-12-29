"""
08_model_competition.py
Author: Sean L. Girgis

PURPOSE:
    The "Tournament Layer" of the HorizonScale pipeline. This script acts as the 
    arbiter between different forecasting engines (Prophet vs. XGBoost). It 
    conducts a rigorous backtest audit to select the "Champion" model for each 
    individual asset based on historical accuracy.

TOURNAMENT LOGIC:
    - Backtest Audit: Compares predictions against real values for a 4-month hold-out set.
    - Metric: Uses Mean Absolute Percentage Error (MAPE) as the primary KPI.
    - Selection: Assigns the "Champion" status to the model with accuracy_rank = 1.
    - Persistence: Merges winning forecasts into a final production-ready Parquet.

FIX: 
    - Implements explicit column selection in UNION ALL blocks to prevent 
      Binder Errors during DuckDB query execution.
"""

import duckdb
import pandas as pd
import polars as pl
from pathlib import Path

# Project-specific internal libraries
from horizonscale.lib.config import DB_PATH, MASTER_DATA_DIR
from horizonscale.lib.logging import init_root_logging, execution_timer

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

# Path Normalization for DuckDB SQL compatibility
PROPHET_PARQUET = str(MASTER_DATA_DIR / "prophet_turbo_master.parquet").replace("\\", "/")
CHALLENGER_PARQUET = str(MASTER_DATA_DIR / "challenger_forecast_master.parquet").replace("\\", "/")
CHAMPION_PARQUET = MASTER_DATA_DIR / "champion_forecast_master.parquet"

def ensure_tables_exist(con):
    """
    Ensures that the model results are registered in the DuckDB session 
    before the tournament begins.
    """
    tables = [t[0] for t in con.execute("SELECT table_name FROM information_schema.tables").fetchall()]
    if "prophet_results" not in tables:
        con.execute(f"CREATE TABLE prophet_results AS SELECT * FROM read_parquet('{PROPHET_PARQUET}')")
    if "challenger_results" not in tables:
        con.execute(f"CREATE TABLE challenger_results AS SELECT * FROM read_parquet('{CHALLENGER_PARQUET}')")

def run_model_tournament():
    """
    Orchestrates the model competition by calculating errors and 
    extracting the final champion forecasts.
    """
    with execution_timer(logger, "Model Competition & Backtest Audit"):
        con = duckdb.connect(str(DB_PATH))
        ensure_tables_exist(con)

        # Resetting the leaderboard for a fresh audit
        con.execute("DROP TABLE IF EXISTS model_leaderboard")
        con.execute("DROP TABLE IF EXISTS final_champion_forecasts")

        # 1. AUDIT PHASE: Calculating MAPE for 16,000+ series
        # Uses a UNION ALL with explicit schema alignment to avoid Binder Errors.
        logger.info("Auditing 16,000 series with synced schemas...")
        con.execute("""
            CREATE TABLE model_leaderboard AS
            WITH combined_results AS (
                SELECT ds, yhat, host_id, resource, 'Prophet' as model_type FROM prophet_results WHERE data_type = 'backtest'
                UNION ALL
                SELECT ds, yhat, host_id, resource, 'XGBoost' as model_type FROM challenger_results WHERE data_type = 'backtest'
            ),
            error_calc AS (
                SELECT 
                    r.host_id, r.resource, r.model_type,
                    AVG(ABS(r.yhat - p.y) / NULLIF(p.y, 0)) * 100 as mape
                FROM combined_results r
                JOIN processed_data p ON r.ds = p.ds AND r.host_id = p.host_id AND r.resource = p.resource
                GROUP BY 1, 2, 3
            )
            SELECT *, RANK() OVER (PARTITION BY host_id, resource ORDER BY mape ASC) as accuracy_rank
            FROM error_calc
        """)

        # 2. SELECTION PHASE: Extracting champion forecasts
        # Joins winning model metadata with the full forecast sets.
        logger.info("Selecting winning forecasts...")
        con.execute("""
            CREATE TABLE final_champion_forecasts AS
            WITH winners AS (
                SELECT host_id, resource, model_type FROM model_leaderboard WHERE accuracy_rank = 1
            ),
            all_forecasts AS (
                SELECT ds, yhat, yhat_lower, yhat_upper, host_id, resource, 'Prophet' as source_model FROM prophet_results
                UNION ALL
                SELECT ds, yhat, yhat_lower, yhat_upper, host_id, resource, 'XGBoost' as source_model FROM challenger_results
            )
            SELECT f.*, w.model_type as winning_model
            FROM all_forecasts f
            JOIN winners w ON f.host_id = w.host_id AND f.resource = w.resource AND f.source_model = w.model_type
        """)

        # 3. PERSISTENCE & ANALYTICS
        # Exports the final champion dataset and logs tournament standings.
        master_df = con.execute("SELECT * FROM final_champion_forecasts").df()
        pl.from_pandas(master_df).write_parquet(CHAMPION_PARQUET)
        
        stats = con.execute("""
            SELECT model_type, COUNT(*), AVG(mape) 
            FROM model_leaderboard WHERE accuracy_rank = 1 GROUP BY 1
        """).fetchall()
        
        logger.info("--- FINAL TOURNAMENT STANDINGS ---")
        for row in stats:
            logger.info(f"Winner: {row[0]} | Servers Won: {row[1]} | Avg MAPE: {row[2]:.2f}%")
        
        con.close()
        logger.info(f"SUCCESS: Champion dataset finalized.")

if __name__ == "__main__":
    run_model_tournament()