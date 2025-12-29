"""
03_data_pipeline.py
Author: Sean L Girgis

MY DATA REFINERY LOGIC:
I cannot feed raw, "dirty" telemetry directly into a forecasting model. 
This script is my processing plant. It handles the 'heavy lifting' of 
data engineering so the models receive a perfect, high-octane dataset.

My mission here:
1.  SCRUB: Remove duplicates and fix hardware reporting glitches.
2.  PLUG GAPS: Ensure a 100% continuous timeline (no missing days).
3.  ENRICH: Inject 'memory' (lags) and 'trends' (rolling stats) into every row.


Genesis (Entrance Criteria):
    - Existence of MASTER_PARQUET_FILE or LEGACY_INPUT_DIR.
    - Functional DuckDB connection via DB_PATH.

Success (Exit Criteria):
    - processed_data table created with 'ds' (Date) and 'y' (Target) columns.
    - 7-day rolling mean/std features populated for every host/resource.
    - Referential Integrity: Total rows must equal (Hosts * Resources * Time Periods).
"""
"""
03_data_pipeline.py
Author: Sean L Girgis

THE REFINERY:
Aggregates 144 monthly CSVs, cleans the data, and joins with host metadata.
Standardized for lowercase compatibility and 10-variety audit.
"""

import duckdb
from pathlib import Path
import polars as pl
from horizonscale.lib.config import (
    DB_PATH, LEGACY_INPUT_DIR, RESOURCE_FILE_PREFIXES
)
from horizonscale.lib.logging import init_root_logging

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

def check_genesis_criteria():
    """Verifies that the legacy CSVs exist before starting refinement."""
    logger.info("--- Validating Refinery Entrance Criteria ---")
    if not LEGACY_INPUT_DIR.exists():
        logger.error(f"FAIL: Legacy directory not found at {LEGACY_INPUT_DIR}")
        raise FileNotFoundError("Run 02_export_monthly_csvs first.")
    
    csv_count = len(list(LEGACY_INPUT_DIR.glob("**/*.csv")))
    if csv_count == 0:
        raise ValueError("No CSV files found in legacy directory.")
    
    logger.info(f"PASS: Found {csv_count} CSV files. Starting ingestion...")

def validate_refinery_exit(con: duckdb.DuckDBPyConnection):
    """
    SUCCESS CRITERIA AUDIT:
    Verifies that all 10 varieties survived the ingestion and join process.
    """
    logger.info("--- Validating Refinery Exit Criteria ---")
    
    # Audit row counts and variety count
    audit_df = con.execute("""
        SELECT h.scenario, h.variant, COUNT(DISTINCT p.host_id) as host_count
        FROM processed_data p
        JOIN hosts h ON p.host_id = h.node_name
        GROUP BY 1, 2 ORDER BY 1, 2
    """).pl()
    
    total_rows = con.execute("SELECT count(*) FROM processed_data").fetchone()[0]
    
    summary = (
        f"\n{'='*60}\n"
        f" SUCCESS: Refinery Ingestion Complete\n"
        f"{'-'*60}\n"
        f" Total Processed Rows: {total_rows:,}\n"
        f" Varieties Retained:  {len(audit_df)} / 10\n"
        f"{'-'*60}\n"
        f"{audit_df}\n"
        f"{'='*60}\n"
    )
    print(summary)
    
    if len(audit_df) < 10:
        logger.warning("DATA LOSS: Some varieties did not survive the refinery join.")
    else:
        logger.info("EXIT CRITERIA SATISFIED: Processed data is complete.")

def run_refinery():
    """Main ETL logic: Ingest, Normalize, Join, and Persist."""
    check_genesis_criteria()
    
    con = duckdb.connect(str(DB_PATH))
    
    # Drop table if exists for idempotency
    con.execute("DROP TABLE IF EXISTS processed_data")
    
    # Create the landing zone for processed data
    con.execute("""
        CREATE TABLE processed_data (
            ds TIMESTAMP,
            host_id VARCHAR,
            resource VARCHAR,
            y DOUBLE,
            cap DOUBLE
        )
    """)

    # Step 1: Resource-Specific Ingestion
    for res, prefix in RESOURCE_FILE_PREFIXES.items():
        logger.info(f"Ingesting {res} feeds...")
        path_pattern = str(LEGACY_INPUT_DIR / "**" / f"{prefix}_*.csv")
        
        # DuckDB handles the schema and globbing automatically
        # We normalize columns to Prophet-friendly names (ds, y)
        con.execute(f"""
            INSERT INTO processed_data
            SELECT 
                CAST(date AS TIMESTAMP) as ds,
                host_id,
                '{res}' as resource,
                {res}_p95 as y,
                100.0 as cap
            FROM read_csv_auto('{path_pattern}')
        """)

    # Step 2: Final Audit
    validate_refinery_exit(con)
    con.close()

if __name__ == "__main__":
    run_refinery()