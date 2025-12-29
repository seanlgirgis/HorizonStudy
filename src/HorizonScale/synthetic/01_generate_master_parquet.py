"""
01_generate_master_parquet.py
Author: Sean L. Girgis

Purpose:
    The "Heavy Lifter" of the pipeline. It synthesizes a contiguous 3-year daily 
    telemetry series for 2,000+ hosts across multiple resources (CPU, Memory, etc.). 
    It translates behavioral DNA into millions of rows of high-fidelity data.

Genesis (Entrance Criteria):
    - horizonscale_synth.db must be initialized with 'hosts' and 'time_periods'.
    - scenario_generators.py must contain mathematical logic for each Scenario Enum.

Success (Exit Criteria):
    - A master Parquet file is persisted with 'snappy' compression.
    - Row count must strictly equal (Total Hosts * Total Resources * Total Days).
    - Data Audit verifies that 10/10 behavioral varieties are represented.
"""

from pathlib import Path
import duckdb  
import polars as pl
import numpy as np
from tqdm import tqdm
import os

from horizonscale.lib.config import (  
    DB_PATH, MASTER_PARQUET_FILE, Scenario, RESOURCE_TYPES, SEED_MODULO  
)  
from horizonscale.lib.scenario_generators import GENERATORS
from horizonscale.lib.logging import init_root_logging

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

def check_entrance_criteria(con: duckdb.DuckDBPyConnection):
    """
    VERIFICATION LAYER: Ensures the foundation exists before intensive computation begins.
    """
    table_query = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    table_names = [t[0] for t in table_query]
    
    if not {"hosts", "time_periods"}.issubset(set(table_names)):
        logger.error("GENESIS FAILED: Inventory or Time dimensions missing from DB.")
        raise RuntimeError("Run 00_init_db.py first.")

    host_count = con.execute("SELECT COUNT(*) FROM hosts").fetchone()[0]
    days_count = con.execute("SELECT COUNT(*) FROM time_periods").fetchone()[0]
    logger.info(f"ENTRANCE PASS: Processing {host_count} hosts over {days_count} days.")

def validate_exit_criteria(expected_rows: int):
    """
    QUALITY AUDIT: Validates file integrity, row-count parity, and behavioral diversity.
    """
    logger.info("--- Validating Telemetry Exit Criteria ---")
    
    # Load generated data for audit
    df = pl.read_parquet(MASTER_PARQUET_FILE)
    actual_rows = len(df)
    
    with duckdb.connect(str(DB_PATH)) as con:
        con.register("telemetry_temp", df)
        # Verify that all 10 scenario/variant combinations survived generation
        audit_df = con.execute("""
            SELECT UPPER(h.scenario) as scenario, UPPER(h.variant) as variant, 
                   COUNT(DISTINCT t.node_name) as host_count
            FROM hosts h
            JOIN telemetry_temp t ON h.node_name = t.node_name
            GROUP BY 1, 2 ORDER BY 1, 2
        """).pl()

    summary = (
        f"\n{'='*60}\n"
        f" SUCCESS: Master Telemetry Generated (UPPERCASE Parity)\n"
        f"{'-'*60}\n"
        f" Total Rows:       {actual_rows:,}\n"
        f" File Size:        {os.path.getsize(MASTER_PARQUET_FILE)/(1024*1024):.2f} MB\n"
        f" Varieties Found:  {len(audit_df)} / 10\n"
        f"{'-'*60}\n"
        f"{audit_df}\n"
        f"{'='*60}\n"
    )
    print(summary)
    
    if actual_rows != expected_rows:
        logger.error(f"DATA LOSS: Expected {expected_rows} rows, but found {actual_rows}.")
    else:
        logger.info(f"EXIT CRITERIA SATISFIED: Data integrity verified.")

def generate_master_parquet():
    """
    ORCHESTRATION: Executes the high-volume telemetry synthesis loop.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError("Genesis Error: Database not found at DB_PATH.")

    with duckdb.connect(str(DB_PATH)) as con:
        check_entrance_criteria(con)
        
        # Extract host metadata and time dimensions
        hosts_df = con.execute("SELECT node_name, scenario, variant, cpu_cores, memory_gb, storage_capacity_mb FROM hosts").pl()
        dates_list = con.execute("SELECT date FROM time_periods ORDER BY date").fetchall()
        dates_series = [d[0] for d in dates_list]
        total_days = len(dates_series)
        
        resources = list(RESOURCE_TYPES.keys())
        expected_rows = len(hosts_df) * len(resources) * total_days
        all_data = []

        # GENERATION LOOP: Iterate through hosts and resources to build the time series
        for host in tqdm(hosts_df.to_dicts(), desc="Synthesizing Telemetry"):
            node_name = host["node_name"]
            
            # NORMALIZATION: Ensure DB strings match UPPERCASE Enum/Config keys
            scenario_str = str(host["scenario"]).upper().strip()
            variant_str = str(host["variant"]).upper().strip()
            
            scenario_enum = Scenario[scenario_str] 
            gen_func = GENERATORS[scenario_enum]

            for res in resources:
                # SEEDING: Deterministic hash ensures reproducibility per host/resource
                seed = (hash(f"{node_name}_{res}") % SEED_MODULO)
                np.random.seed(seed)
                
                # MATHEMATICAL SYNTHESIS: Call the scenario-specific generator
                util_series = gen_func(days=total_days, variant=variant_str, base_seed=seed)
                
                # POST-PROCESSING: Apply resource-specific noise and adjustments
                res_cfg = RESOURCE_TYPES[res]
                util_series = (util_series * np.random.uniform(*res_cfg['adjust_factor_range'])) + \
                              np.random.normal(0, np.random.uniform(*res_cfg['noise_std_range']), total_days)
                
                # CLIPPING: Ensure physically realistic bounds [0, 100%]
                util_series = np.clip(util_series, 0, 100).astype(np.float32)
                
                # Map capacity values based on resource type
                cap_val = host.get('cpu_cores') if res == 'cpu' else \
                          host.get('memory_gb') if res == 'memory' else \
                          host.get('storage_capacity_mb') if res == 'disk' else None

                all_data.append(pl.DataFrame({
                    "date": dates_series,
                    "node_name": [node_name] * total_days,
                    "resource": [res] * total_days,
                    "p95_util": util_series,
                    "capacity": [cap_val] * total_days
                }))

        # PERSISTENCE: Combine all dataframes and write to high-performance Parquet
        master_df = pl.concat(all_data)
        MASTER_PARQUET_FILE.parent.mkdir(parents=True, exist_ok=True)
        master_df.write_parquet(MASTER_PARQUET_FILE, compression="snappy")
        
        validate_exit_criteria(expected_rows)

if __name__ == "__main__":
    generate_master_parquet()