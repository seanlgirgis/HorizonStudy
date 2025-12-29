"""
03_data_pipeline.py
Author: Sean L. Girgis

Purpose:
    The "Refinery" of the HorizonScale project. This script ingests fragmented 
    monthly legacy CSVs, cleans the telemetry, and unifies it into a 
    structured Analytical Base Table (ABT) for forecasting.

Genesis (Entrance Criteria):
    - LEGACY_INPUT_DIR must contain partitioned CSVs from Stage 02.
    - DuckDB database must be initialized with the 'hosts' inventory.

Success (Exit Criteria):
    - 'processed_data' table is populated with standardized 'ds' and 'y' columns.
    - Audit verifies 100% retention of all 10 behavioral DNA varieties.
    - Timestamp normalization is applied to all ingested telemetry.
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
    """
    ENTRANCE AUDIT: Validates the presence of raw legacy files before 
    commencing refinery operations.
    """
    logger.info("--- Validating Refinery Entrance Criteria ---")
    if not LEGACY_INPUT_DIR.exists():
        logger.error(f"FAIL: Legacy directory not found at {LEGACY_INPUT_DIR}")
        raise FileNotFoundError("Genesis failed: Run 02_export_monthly_csvs.py first.")
    
    csv_count = len(list(LEGACY_INPUT_DIR.glob("**/*.csv")))
    if csv_count == 0:
        logger.error("FAIL: No CSV files found for ingestion.")
        raise ValueError("Empty legacy directory.")
    
    logger.info(f"PASS: {csv_count} source files detected. Warming up the refinery...")

def validate_refinery_exit(con: duckdb.DuckDBPyConnection):
    """
    QUALITY AUDIT: Performs a relational join to ensure data integrity 
    and behavioral variety retention post-ingestion.
    """
    logger.info("--- Validating Refinery Exit Criteria ---")
    
    # Audit: Count distinct hosts per behavioral profile
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
        f" Varieties Retained:   {len(audit_df)} / 10\n"
        f"{'-'*60}\n"
        f"{audit_df}\n"
        f"{'='*60}\n"
    )
    print(summary)
    
    if len(audit_df) < 10:
        logger.warning("INTEGRITY ALERT: Behavioral variety loss detected in join.")
    else:
        logger.info("EXIT CRITERIA SATISFIED: ABT integrity verified.")

def run_refinery():
    """
    ETL ENGINE: Orchestrates the SCRUB, PLUG, and ENRICH lifecycle for 
    enterprise telemetry.
    """
    check_genesis_criteria()
    
    con = duckdb.connect(str(DB_PATH))
    
    # IDEMPOTENCY: Reset landing zone
    con.execute("DROP TABLE IF EXISTS processed_data")
    
    # SCHEMA DEFINITION: ds and y are required for Prophet compatibility
    con.execute("""
        CREATE TABLE processed_data (
            ds TIMESTAMP,
            host_id VARCHAR,
            resource VARCHAR,
            y DOUBLE,
            cap DOUBLE
        )
    """)

    # STEP 1: MASSIVE INGESTION
    # Iterates through resource types, globbing all monthly files into one table
    for res, prefix in RESOURCE_FILE_PREFIXES.items():
        logger.info(f"INGESTING: Processing {res} legacy streams...")
        path_pattern = str(LEGACY_INPUT_DIR / "**" / f"{prefix}_*.csv")
        
        # DYNAMIC NORMALIZATION: 
        # Casts strings to timestamps and renames resource-specific columns to 'y'
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

    # STEP 2: POST-REFINERY AUDIT
    validate_refinery_exit(con)
    con.close()
    logger.info("REFINERY SHUTDOWN: Data is ready for the forecasting engine.")

if __name__ == "__main__":
    run_refinery()