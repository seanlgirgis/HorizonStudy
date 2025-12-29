"""
02_export_monthly_csvs.py
Author: Sean L. Girgis

Purpose:
    Simulates a 'Legacy Data Feed' by decomposing the Master Parquet file into 
    fragmented monthly CSVs. This represents the raw, "dirty" state of data 
    before centralization—essential for demonstrating pipeline resilience.

Genesis (Entrance Criteria):
    - Master Parquet must exist at MASTER_PARQUET_FILE (from Stage 01).
    - Resource prefixes and capacity fields must be defined in config.py.

Success (Exit Criteria):
    - Data is partitioned into a hierarchical /YYYY/MM/ directory structure.
    - Every host/resource/day record is accounted for (Row Parity).
    - Files are named according to legacy standards (e.g., cpu_utilization_202401.csv).
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
    """
    ENTRANCE AUDIT: Validates that the source data exists and target 
    directories are initialized.
    """
    logger.info("--- Validating Script Entrance Criteria ---")
    if not MASTER_PARQUET_FILE.exists():
        logger.error(f"FAIL: Master Parquet not found at {MASTER_PARQUET_FILE}")
        raise FileNotFoundError("Genesis failed: Run 01_generate_master_parquet.py first.")
    
    LEGACY_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"PASS: Source data detected. Exporting legacy feeds to {LEGACY_INPUT_DIR}")

def validate_exit_criteria(expected_rows: int):
    """
    QUALITY AUDIT: Performs a post-generation integrity check to ensure 
    referential parity between the Parquet source and the CSV output.
    """
    logger.info("--- Validating Legacy CSV Exit Criteria ---")
    
    actual_rows = 0
    csv_files = list(LEGACY_INPUT_DIR.glob("**/*.csv"))
    
    # Efficiently aggregate row counts from the fragmented library
    for csv_file in csv_files:
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
        logger.error(f"DATA LOSS DETECTED: Source had {expected_rows}, but CSVs have {actual_rows}")
        raise ValueError("Referential Integrity Failure in Export.")

def export_legacy_feeds():
    """
    DECOMPOSITION ENGINE: Maps centralized Parquet data back into 
    resource-specific monthly partitions.
    """
    check_genesis_criteria()
    
    logger.info("LOADING: Reading Master Parquet into memory...")
    master_df = pl.read_parquet(MASTER_PARQUET_FILE)
    total_expected = master_df.height

    # Inject temporal partitioning columns
    master_df = master_df.with_columns([
        pl.col("date").dt.strftime("%Y").alias("year"),
        pl.col("date").dt.strftime("%m").alias("month"),
        pl.col("date").dt.strftime("%Y%m").alias("yearmonth")
    ])

    yearmonths = master_df["yearmonth"].unique().sort().to_list()
    resources = list(RESOURCE_TYPES.keys())

    # Nested Loop: YearMonth -> Resource Type
    for ym in tqdm(yearmonths, desc="Fragmenting Data"):
        year, month = ym[:4], ym[4:]
        month_df = master_df.filter(pl.col("yearmonth") == ym)
        
        for res in resources:
            prefix = RESOURCE_FILE_PREFIXES[res]
            capacity_label = CAPACITY_FIELDS[res]
            
            target_dir = LEGACY_INPUT_DIR / year / month
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # FORMATTING: Transform to the "Legacy" schema
            # Renames generic p95 column to resource-specific names (e.g., cpu_p95)
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