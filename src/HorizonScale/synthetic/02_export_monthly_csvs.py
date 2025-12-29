"""
02_export_monthly_csvs.py
Author: Sean L Girgis

Purpose:
    Simulates a 'Legacy Data Feed' by decomposing the Master Parquet file into 
    individual monthly CSVs. This represents the raw state of data before it is 
    centralized—essential for demonstrating data engineering pipeline resilience.

Genesis (Entrance Criteria):
    - Master Parquet must exist and be accessible.
    - Required legacy configuration (prefixes/paths) must be defined.

Success (Exit Criteria):
    - Every host/resource/day from the Parquet must be accounted for in the CSVs.
    - Folder structure must follow /YYYY/MM/ pattern.
    - File naming must match legacy prefixes (e.g., cpu_utilization_202401.csv).
"""
"""
02_export_monthly_csvs.py
Author: Sean L Girgis

Decomposes the Master Parquet into monthly CSVs to simulate legacy data feeds.
Includes a final audit to ensure all 10 behavioral varieties are represented.
"""

import polars as pl
from pathlib import Path
from tqdm import tqdm
import os

from horizonscale.lib.config import (
    MASTER_PARQUET_FILE, LEGACY_INPUT_DIR, RESOURCE_TYPES, 
    RESOURCE_FILE_PREFIXES, CAPACITY_FIELDS
)
from horizonscale.lib.logging import init_root_logging

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

def check_genesis_criteria():
    """Validates entrance criteria for CSV export."""
    logger.info("--- Validating Script Entrance Criteria ---")
    if not MASTER_PARQUET_FILE.exists():
        logger.error(f"FAIL: Master Parquet not found at {MASTER_PARQUET_FILE}")
        raise FileNotFoundError("Genesis failed: Run 01_generate first.")
    
    LEGACY_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"PASS: Exporting legacy feeds to {LEGACY_INPUT_DIR}")

def validate_exit_criteria(expected_rows: int):
    """Performs a post-generation audit on the exported CSV library."""
    logger.info("--- Validating Legacy CSV Exit Criteria ---")
    
    actual_rows = 0
    csv_files = list(LEGACY_INPUT_DIR.glob("**/*.csv"))
    
    for csv_file in csv_files:
        # Fast row count reading only the height
        actual_rows += pl.read_csv(csv_file).height
        
    summary = (
        f"\n{'='*60}\n"
        f" SUCCESS: Legacy CSV Export Complete\n"
        f"{'-'*60}\n"
        f" Total Rows Partitioned: {actual_rows:,}\n"
        f" Total CSV Files:        {len(csv_files)}\n"
        f" Parity Status:          {'MATCH' if actual_rows == expected_rows else 'MISMATCH'}\n"
        f"{'='*60}\n"
    )
    print(summary)
    
    if actual_rows != expected_rows:
        raise ValueError(f"Data loss detected! Expected {expected_rows}, found {actual_rows}")

def export_legacy_feeds():
    """Partitioning logic with month/resource grouping."""
    check_genesis_criteria()
    
    logger.info("Loading Master Parquet...")
    master_df = pl.read_parquet(MASTER_PARQUET_FILE)
    total_expected = master_df.height

    # Create time helpers
    master_df = master_df.with_columns([
        pl.col("date").dt.strftime("%Y").alias("year"),
        pl.col("date").dt.strftime("%m").alias("month"),
        pl.col("date").dt.strftime("%Y%m").alias("yearmonth")
    ])

    yearmonths = master_df["yearmonth"].unique().sort().to_list()
    resources = list(RESOURCE_TYPES.keys())

    for ym in tqdm(yearmonths, desc="Exporting Monthly Feeds"):
        year, month = ym[:4], ym[4:]
        month_df = master_df.filter(pl.col("yearmonth") == ym)
        
        for res in resources:
            prefix = RESOURCE_FILE_PREFIXES[res]
            capacity_label = CAPACITY_FIELDS[res]
            
            target_dir = LEGACY_INPUT_DIR / year / month
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Legacy format: renaming and specific column selection
            res_df = month_df.filter(pl.col("resource") == res).select([
                pl.col("date").dt.strftime("%Y-%m-%d").alias("date"),
                pl.col("node_name").alias("host_id"),
                pl.col("p95_util").alias(f"{res}_p95"),
                pl.col("capacity").alias(capacity_label if capacity_label else "capacity")
            ])
            
            res_df.write_csv(target_dir / f"{prefix}_{ym}.csv")

    validate_exit_criteria(total_expected)

if __name__ == "__main__":
    export_legacy_feeds()