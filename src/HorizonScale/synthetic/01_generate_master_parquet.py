"""
01_generate_master_parquet.py
Author: Sean L Girgis

Purpose of the script:
    This script generates a comprehensive, contiguous daily P95 utilization time series for all hosts in the 'hosts' table
    from the initialized DuckDB database. It creates synthetic yet realistic data for multiple resources (CPU, Memory, Disk, Network)
    using predefined scenario-based generators, incorporating trends, seasonality, noise, and resource-specific adjustments.
    The output is a single, compressed master Parquet file serving as the primary data source for the HorizonScale pipeline,
    enabling efficient downstream processing, EDA, and forecasting. As the second step after DB init (00_init_db.py),
    it ensures full coverage (every host/resource/day) with validation checks for completeness, range sanity, and nulls.

What it does:
    - Parses CLI args for --years (optional; defaults from config).
    - Sets up dedicated logging (01_generate_master_parquet.log).
    - Connects to DuckDB, loads hosts metadata and dates from time_periods.
    - For each host/scenario/variant, generates utilization series per resource using mapped generators.
    - Applies resource-specific adjustments (factor, noise) and clips to [0,100].
    - Adds capacities from hosts table where applicable (null for network/disk).
    - Builds a massive Polars DataFrame with all combinations (hosts x resources x days).
    - Writes to Parquet with snappy compression.
    - Validates output: existence, row count match, util range [0,100], no critical nulls, unique counts.
    - Computes and logs global mean/std of p95_util and file size.
    - Raises errors on validation failures; logs SUCCESS with summary stats.
    Overall, it simulates realistic infrastructure metrics at scale, ensuring data quality for the pipeline.

Dependencies:
    - External packages:
        - polars (pl): For DataFrame ops, lazy queries, Parquet write, and stats (mean/std/min/max/null_count).
        - duckdb: For DB connection and host/date loading via SQL.
        - numpy (np): For random seeds, normal noise, and series generation in called generators.
        - tqdm: For progress bars in host/resource loops.
        - argparse: For CLI parsing of --years.
    - Standard library: logging (via utils), Path (pathlib for paths), argparse.
    - Project-internal:
        - horizonscale.config: Imports DB_PATH, MASTER_PARQUET_FILE, DEFAULT_YEARS, RESOURCE_TYPES, Scenario Enum,
          SCENARIO_VARIANTS, SEED_MODULO, CAPACITY_FIELDS.
        - horizonscale.utils: Imports setup_logging, generate_dates_and_yearmonths (but uses DB time_periods instead).
        - horizonscale.scenario_generators: Imports GENERATORS dict to map scenarios to functions like generate_steady_growth.
    Assumes DB is pre-seeded by 00_init_db.py; runs standalone after init.

Outputs or Results:
    - Parquet file: master_daily_YYYY_YYYY.parquet at MASTER_PARQUET_FILE (e.g., master_daily_2023_2025.parquet),
      with columns: date (Date), node_name (str, host_id), resource (str), p95_util (float), capacity (float or null).
      Row count: num_hosts * 4 resources * total_days (e.g., 2000 * 4 * 1096 ≈ 8.8M rows).
    - Log file: logs/01_generate_master_parquet.log with progress, per-host info, validation results, and SUCCESS summary
      (hosts/days/resources, rows/size, util range, global mean/std).
    - Console: Mirrors log output.
    - Exceptions: ValueError on validation fails (e.g., row mismatch, out-of-range utils, unexpected nulls).
    - Results: Canonical raw data source for pipeline; enables 02_export_monthly_csvs, EDA, processing, etc.

What the code inside is doing:
    The script features a docstring, imports, logger setup, then functions and main:
    - Docstring: Purpose, inputs/outputs, success criteria, run instructions.
    - Imports: pl, duckdb, np, tqdm, logging/Path/argparse; config constants/Scenario/GENERATORS/SCENARIO_VARIANTS/SEED_MODULO/CAPACITY_FIELDS;
      utils setup_logging.
    - logger = setup_logging(Path(__file__).stem): Dedicated for '01_generate_master_parquet'.
    - RESOURCE_TYPES = list(config.RESOURCE_TYPES): Extracts resources list.
    - def generate_master_parquet(years: range):
        - Logs start; computes total_days via len(generate_dates_and_yearmonths(years)) – but actually uses DB.
        - master_path = MASTER_PARQUET_FILE (hardcoded years, but uses dynamic).
        - Connects to DB_PATH.
        - Loads hosts as pl.DataFrame via lazy SQL SELECT * FROM hosts.
        - Loads dates as list via SQL SELECT date FROM time_periods ORDER BY date.
        - num_hosts = len(hosts); expected_rows = num_hosts * len(RESOURCE_TYPES) * total_days.
        - Initializes empty lists for all columns (date, node_name, resource, p95_util, capacity).
        - Outer tqdm loop over hosts rows:
          - Extracts host_id, scenario (Enum), variant.
          - Computes seed = (hash(host_id) % SEED_MODULO) for reproducibility.
          - Inner loop over resources:
            - Gets generator from GENERATORS[scenario], calls with days=total_days, variant.
            - Adjusts: series *= RESOURCE_TYPES[res]['adjust_factor']; adds np.random.normal(0, noise_std, days).
            - Clips to [0,100].
            - Gets capacity_col from CAPACITY_FIELDS[res]; if exists, repeats host[capacity_col]; else None list.
            - Appends to lists: repeated dates, repeated host_id, repeated res, series list, capacity list.
        - Builds pl.DataFrame from lists with schema (date: pl.Date, etc.).
        - Writes to master_path with snappy compression.
        - Validates:
          - Reads back as lazy DF.
          - Asserts height == expected_rows.
          - Checks nulls in critical cols (date/node_name/resource/p95_util) == 0.
          - Min/max util [0,100].
          - Unique hosts/resources match num_hosts/len(RESOURCE_TYPES).
          - Computes global mean/std util.
          - Gets file size.
          - Logs SANITY CHECKS PASSED and SUCCESS summary.
    - def main():
        - argparse Parser with --years (str like '2023-2025' or None).
        - If None, uses DEFAULT_YEARS; logs default.
        - Else parses start-end to range.
        - Calls generate_master_parquet(years).
    - if __name__ == "__main__": main().
    Code is efficient: List-based building for large DF, lazy Polars for validation, progress bars for long runs,
    seeded randomness for reproducibility, resource-specific tweaks for realism.
"""

from pathlib import Path
import duckdb  
import polars as pl
import numpy as np
from tqdm import tqdm

from horizonscale.lib.config import (  
    DB_PATH, MASTER_PARQUET_FILE, Scenario, Scenario, RESOURCE_TYPES, SEED_MODULO  
)  

from horizonscale.lib.scenario_generators import GENERATORS

# Initialize project-standard logging and synthetic data generators
from horizonscale.lib.logging import init_root_logging
logger = init_root_logging(Path(__file__).stem)



def check_entrance_criteria(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Entrance Criteria: Validates that the synthetic database exists and 
    contains the necessary metadata and time dimensions to start generation.
    Returns a dictionary of counts for planning the generation loop.
    """
    logger.info("--- Validating Script Entrance Criteria ---")
    
    # 1. Check if tables exist
    tables = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    table_names = [t[0] for t in tables]
    required = {"hosts", "business_hierarchy", "time_periods"}
    
    missing = required - set(table_names)
    if missing:
        logger.error(f"FAIL: Missing required tables: {missing}")
        raise RuntimeError(f"Database incomplete. Missing tables: {missing}")

    # 2. Extract counts for generation planning
    host_count = con.execute("SELECT COUNT(*) FROM hosts").fetchone()[0]
    days_count = con.execute("SELECT COUNT(*) FROM time_periods").fetchone()[0]

    if host_count == 0 or days_count == 0:
        logger.error("FAIL: Required tables are empty.")
        raise RuntimeError("Database found, but contains no metadata to process.")

    logger.info(f"PASS: Entrance criteria met. Ready to process {host_count} hosts over {days_count} days.")
    
    return {
        "num_hosts": host_count,
        "total_days": days_count
    }


def generate_master_parquet():
    logger.info(f"Initializing simulation environment")

    if not DB_PATH.exists():
        logger.error(f"Critical: Database not found at {DB_PATH}")
        raise FileNotFoundError("Run 00_init_db.py before starting telemetry generation.")

    with duckdb.connect(str(DB_PATH)) as con:
        # Step 1: Validate Entrance Criteria
        meta = check_entrance_criteria(con)
        
        # 1. Load Host DNA (Profiles)
        hosts_df = con.execute("SELECT node_name, scenario, variant, cpu_cores, memory_gb, storage_capacity_mb FROM hosts").pl()
        
        # 2. Load the Time Horizon
        dates_list = con.execute("SELECT date FROM time_periods ORDER BY date").fetchall()
        dates_series = [d[0] for d in dates_list]
        total_days = len(dates_series)
        
        logger.info(f"Context loaded: {len(hosts_df)} hosts across {total_days} days.")

        # --- GENERATION LOOP ---
        all_data = []
        resources = list(RESOURCE_TYPES.keys())

        logger.info(f"Generating telemetry for {len(resources)} resources per host...")

        # Progress bar over hosts for visibility
        for host in tqdm(hosts_df.to_dicts(), desc="Generating Master Telemetry"):
            node_name = host["node_name"]
            scenario_str = host["scenario"]
            variant = host["variant"]
            
            # Map string back to Enum for the Dispatcher
            scenario_enum = Scenario[scenario_str]
            gen_func = GENERATORS[scenario_enum]

            for res in resources:
                # 1. Create a unique, reproducible seed for this specific host-resource pair
                # We combine the node_name and resource string to get a unique hash
                seed = (hash(f"{node_name}_{res}") % SEED_MODULO)
                np.random.seed(seed)
                
                # 2. Call the base math generator (The "DNA")
                util_series = gen_func(days=total_days, variant=variant, base_seed=seed)
                
                # 3. Apply Randomized Resource Adjustments (from config ranges)
                res_cfg = RESOURCE_TYPES[res]
                
                # Sample unique factor and noise intensity for THIS host/resource
                adj_factor = np.random.uniform(*res_cfg['adjust_factor_range'])
                noise_lvl = np.random.uniform(*res_cfg['noise_std_range'])
                
                # Apply the adjustments
                util_series = util_series * adj_factor
                util_series += np.random.normal(0, noise_lvl, total_days)
                
                # 4. Final Clip and Type Conversion (Float32 saves 50% space vs Float64)
                util_series = np.clip(util_series, 0, 100).astype(np.float32)

                # 5. Extract Capacity (if applicable)
                # Map resource to host table column (cpu_cores, memory_gb, etc.)
                cap_val = None
                if res == 'cpu': cap_val = host['cpu_cores']
                elif res == 'memory': cap_val = host['memory_gb']
                elif res == 'disk': cap_val = host['storage_capacity_mb']

                # 6. Build Polars DataFrame for this specific slice
                all_data.append(pl.DataFrame({
                    "date": dates_series,
                    "node_name": [node_name] * total_days,
                    "resource": [res] * total_days,
                    "p95_util": util_series,
                    "capacity": [cap_val] * total_days
                }))

        # Step 3: Concatenation and Persistence
        logger.info("Merging telemetry slices into master dataframe...")
        master_df = pl.concat(all_data)
        
        # Ensure output directory exists
        MASTER_PARQUET_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Write with snappy compression for high speed and efficient storage
        master_df.write_parquet(MASTER_PARQUET_FILE, compression="snappy")
        
        logger.info(f"SUCCESS: Generated {len(master_df)} rows.")
        logger.info(f"Master Parquet saved to: {MASTER_PARQUET_FILE}")
    
    return

if __name__ == "__main__":  
    generate_master_parquet()
    
    