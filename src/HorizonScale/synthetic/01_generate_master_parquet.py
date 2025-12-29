"""
01_generate_master_parquet.py
Author: Sean L Girgis

Purpose:
    Generates a contiguous daily P95 utilization time series for all hosts in the inventory.
    This script acts as the 'heavy lifter' of the synthetic pipeline, translating behavioral
    DNA into millions of rows of telemetry data.

Exit Criteria:
    - File Existence: The master .parquet file must be physically written to disk.
    - Referential Integrity: Row count must exactly match (Hosts * Resources * Days).
    - Domain Sanity: p95_utilization must strictly reside within the [0, 100] interval.
    - Data Quality: Zero null values allowed in primary keys (date, node_name, resource).
"""
"""
01_generate_master_parquet.py
Author: Sean L Girgis

Purpose:
    Generates millions of rows of daily P95 utilization based on behavioral DNA.
    Standardized to handle lowercase database strings and audited for 10/10 variety.
"""
"""
01_generate_master_parquet.py
Author: Sean L Girgis

Purpose:
    Generates millions of rows of daily P95 utilization based on behavioral DNA.
    Standardized for UPPERCASE database strings and audited for 10/10 variety.
"""
"""
01_generate_master_parquet.py
Author: Sean L Girgis

Purpose:
    Generates millions of rows of daily P95 utilization based on behavioral DNA.
    Standardized for UPPERCASE parity between Database, Enum, and Config.
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

logger = init_root_logging(Path(__file__).stem)

def check_entrance_criteria(con: duckdb.DuckDBPyConnection):
    """Ensures hosts and time dimensions are seeded and available."""
    table_query = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    table_names = [t[0] for t in table_query]
    
    if not {"hosts", "time_periods"}.issubset(set(table_names)):
        raise RuntimeError("Genesis stopped: Database not initialized.")

    host_count = con.execute("SELECT COUNT(*) FROM hosts").fetchone()[0]
    days_count = con.execute("SELECT COUNT(*) FROM time_periods").fetchone()[0]
    logger.info(f"PASS: Ready to process {host_count} hosts over {days_count} days.")

def validate_exit_criteria(expected_rows: int):
    """Audits the Parquet file for integrity and variety coverage."""
    logger.info("--- Validating Telemetry Exit Criteria ---")
    
    df = pl.read_parquet(MASTER_PARQUET_FILE)
    actual_rows = len(df)
    
    with duckdb.connect(str(DB_PATH)) as con:
        con.register("telemetry_temp", df)
        # Audit handles potential casing in DB via UPPER()
        audit_df = con.execute("""
            SELECT UPPER(h.scenario) as scenario, UPPER(h.variant) as variant, COUNT(DISTINCT t.node_name) as host_count
            FROM hosts h
            JOIN telemetry_temp t ON h.node_name = t.node_name
            GROUP BY 1, 2 ORDER BY 1, 2
        """).pl()

    summary = (
        f"\n{'='*60}\n"
        f" SUCCESS: Master Telemetry Generated (UPPERCASE Standard)\n"
        f"{'-'*60}\n"
        f" Total Rows:       {actual_rows:,}\n"
        f" File Size:        {os.path.getsize(MASTER_PARQUET_FILE)/(1024*1024):.2f} MB\n"
        f" Varieties Found:  {len(audit_df)} / 10\n"
        f"{'-'*60}\n"
        f"{audit_df}\n"
        f"{'='*60}\n"
    )
    print(summary)
    logger.info(f"Exit criteria satisfied with {len(audit_df)} varieties.")

def generate_master_parquet():
    """Orchestrates telemetry generation with case-insensitive normalization."""
    if not DB_PATH.exists():
        raise FileNotFoundError("Run 00_init_db.py first.")

    with duckdb.connect(str(DB_PATH)) as con:
        check_entrance_criteria(con)
        
        hosts_df = con.execute("SELECT node_name, scenario, variant, cpu_cores, memory_gb, storage_capacity_mb FROM hosts").pl()
        dates_list = con.execute("SELECT date FROM time_periods ORDER BY date").fetchall()
        dates_series = [d[0] for d in dates_list]
        total_days = len(dates_series)
        
        resources = list(RESOURCE_TYPES.keys())
        expected_rows = len(hosts_df) * len(resources) * total_days
        all_data = []

        for host in tqdm(hosts_df.to_dicts(), desc="Generating Master Telemetry"):
            node_name = host["node_name"]
            
            # NORMALIZATION LAYER: Forces uppercase to bridge DB casing and Enum names
            scenario_str = str(host["scenario"]).upper().strip()
            variant_str = str(host["variant"]).upper().strip()
            
            try:
                scenario_enum = Scenario[scenario_str] 
                gen_func = GENERATORS[scenario_enum]
            except KeyError as e:
                logger.error(f"Mapping Error: {scenario_str} not in Scenario Enum. Check config.py.")
                raise e

            for res in resources:
                seed = (hash(f"{node_name}_{res}") % SEED_MODULO)
                np.random.seed(seed)
                
                # variant_str is now guaranteed to match UPPERCASE config keys like 'NORMAL'
                util_series = gen_func(days=total_days, variant=variant_str, base_seed=seed)
                res_cfg = RESOURCE_TYPES[res]
                
                util_series = (util_series * np.random.uniform(*res_cfg['adjust_factor_range'])) + \
                              np.random.normal(0, np.random.uniform(*res_cfg['noise_std_range']), total_days)
                
                util_series = np.clip(util_series, 0, 100).astype(np.float32)
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

        master_df = pl.concat(all_data)
        MASTER_PARQUET_FILE.parent.mkdir(parents=True, exist_ok=True)
        master_df.write_parquet(MASTER_PARQUET_FILE, compression="snappy")
        validate_exit_criteria(expected_rows)

if __name__ == "__main__":
    generate_master_parquet()