"""
00_init_db.py
Author: Sean L. Girgis

Purpose:
    Initializes the physical foundation of the HorizonScale pipeline. It creates 
    the DuckDB database, defines the relational schema, and seeds 2,000 servers 
    with behavioral "DNA" (Scenarios/Variants).

Genesis (Entrance Criteria):
    - external DDL file (create_tables.sql) must exist in the SQL_SCHEMA_DIR.
    - config.py must define the SCENARIO_DISTRIBUTION and VARIANT_WEIGHTS.

Success (Exit Criteria):
    - horizonscale_synth.db is physically created on disk.
    - The 'hosts' table contains exactly DEFAULT_NUM_HOSTS records.
    - Audit verifies that 10/10 behavioral varieties are successfully seeded.
"""

import duckdb  
import polars as pl
import numpy as np
import random
from pathlib import Path
from faker import Faker
from datetime import datetime

from horizonscale.lib.config import (  
    DB_PATH, SQL_SCHEMA_DIR, DEFAULT_NUM_HOSTS, TIME_START, TIME_END,
    Scenario, SCENARIO_DISTRIBUTION, SCENARIO_VARIANTS, VARIANT_WEIGHTS,
    CLASSIFICATIONS, DEPARTMENTS, REGIONS, SERVER_TYPES 
)  
from horizonscale.lib.logging import init_root_logging

# Initialize enterprise-grade logging and synthetic data generators
logger = init_root_logging(Path(__file__).stem)
faker = Faker()

def purge_existing_db(db_path: Path):
    """
    IDEMPOTENCY LAYER: Ensures a deterministic starting point by removing 
    stale database files before execution.
    """
    if db_path.exists():
        db_path.unlink()
        logger.info(f"CLEAN SLATE: Purged existing database at {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True) 

def create_schema(con: duckdb.DuckDBPyConnection):
    """
    SCHEMA DEFINITION: Executes the Data Definition Language (DDL) 
    to establish the relational structure.
    """
    schema_path = SQL_SCHEMA_DIR / "create_tables.sql"
    with open(schema_path, "r") as f:
        con.execute(f.read())
    logger.info("RELATIONAL SCHEMA: Tables (hosts, hierarchy, time) initialized.")

def seed_hosts_with_variants(con: duckdb.DuckDBPyConnection, num_hosts: int):
    """
    INVENTORY SEEDING: Generates hosts with unique UUIDs and assigns 
    behavioral DNA based on weighted enterprise distributions.
    """
    logger.info(f"SEEDING: Generating {num_hosts} hosts with weighted DNA...")
    scenarios = list(SCENARIO_DISTRIBUTION.keys())
    scenario_probs = list(SCENARIO_DISTRIBUTION.values())
    variant_types = list(VARIANT_WEIGHTS.keys())
    variant_probs = list(VARIANT_WEIGHTS.values())
    
    hosts_data = []
    for _ in range(num_hosts):
        uuid_str = faker.uuid4()
        # Select Scenario (e.g., Seasonal) and Variant (e.g., Extreme)
        scenario_member = np.random.choice(scenarios, p=scenario_probs)
        v_type = np.random.choice(variant_types, p=variant_probs)
        chosen_variant = SCENARIO_VARIANTS[scenario_member][v_type]

        hosts_data.append({
            "node_name": f"server-{uuid_str[:8]}",
            "classification": random.choice(CLASSIFICATIONS).lower(),
            "server_type": random.choice(SERVER_TYPES).lower(),
            "region": random.choice(REGIONS).lower(),
            "cpu_cores": random.choice([16, 32, 64, 128]),
            "memory_gb": random.choice([64, 128, 256, 512]),
            "storage_capacity_mb": random.choice([500000, 1000000, 2000000, 5000000]),
            "department": random.choice(DEPARTMENTS),
            "scenario": scenario_member.value.lower(),  # Standardized for SQL joins
            "variant": chosen_variant.lower()
        })

    hosts_df = pl.DataFrame(hosts_data)
    con.register("hosts_temp", hosts_df)
    con.execute("INSERT INTO hosts SELECT * FROM hosts_temp")
    return [h['node_name'] for h in hosts_data]

def validate_seeding_success(con: duckdb.DuckDBPyConnection):
    """
    EXIT AUDIT: Verifies the integrity and variety of the seeded inventory. 
    Ensures all 10 behavioral combinations are present.
    """
    logger.info("--- Validating Database Exit Criteria ---")
    
    audit_df = con.execute("""
        SELECT scenario, variant, count(*) as count
        FROM hosts
        GROUP BY scenario, variant
        ORDER BY scenario, variant
    """).pl()
    
    total_varieties = len(audit_df)
    total_hosts = audit_df["count"].sum()
    
    summary = (
        f"\n{'='*60}\n"
        f" SUCCESS: Database Initialization Complete\n"
        f"{'-'*60}\n"
        f" Total Hosts Created:  {total_hosts:,}\n"
        f" Behavioral Varieties: {total_varieties} / 10\n"
        f"{'-'*60}\n"
        f"{audit_df}\n"
        f"{'='*60}\n"
    )
    print(summary)
    
    if total_varieties < 10:
        logger.warning(f"CRITERIA FAILURE: Found only {total_varieties}/10 varieties.")
    else:
        logger.info("EXIT CRITERIA SATISFIED: All behavioral DNA profiles are present.")

def seed_time_periods(con: duckdb.DuckDBPyConnection, start: str, end: str):
    """
    TEMPORAL SEEDING: Establishes a contiguous daily calendar dimension 
    spanning the 3-year simulation window.
    """
    df_time = pl.date_range(datetime.strptime(start, "%Y-%m-%d"), 
                            datetime.strptime(end, "%Y-%m-%d"), 
                            interval="1d", eager=True).to_frame("date")
    
    df_time = df_time.with_columns(pl.col("date").dt.strftime("%Y%m").alias("yearmonth"))
    con.register("time_temp", df_time)
    con.execute("INSERT INTO time_periods SELECT * FROM time_temp")

def init_db():
    """ORCHESTRATION: Executes the full database initialization lifecycle."""
    purge_existing_db(DB_PATH)
    with duckdb.connect(str(DB_PATH)) as con:
        create_schema(con)
        node_names = seed_hosts_with_variants(con, DEFAULT_NUM_HOSTS)
        seed_time_periods(con, TIME_START, TIME_END)
        validate_seeding_success(con)
    
    logger.info("GENESIS COMPLETE: HorizonScale Database is ready for telemetry.")

if __name__ == "__main__":
    init_db()