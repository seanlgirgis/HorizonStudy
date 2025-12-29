"""
00_init_db.py
Author: Sean L Girgis

Purpose:
    Initializes the foundation of the HorizonScale synthetic data pipeline. This script is 
    solely responsible for the physical creation and initial seeding of the DuckDB database.
    It establishes a realistic enterprise inventory of hosts, organizational hierarchy, 
    and time dimensions to support downstream high-fidelity simulations.

What it does:
    - Environment Prep: Purges existing .db files to ensure idempotency and a clean slate.
    - Schema Creation: Executes DDL from external SQL files to define tables (hosts, 
      business_hierarchy, time_periods).
    - Weighted Seeding (Inventory): Generates hosts using a specific Enterprise Profile 
      (e.g., 30% Steady Growth, 5% Breach) and a Risk Profile (80% Common, 20% Rare variants).
    - Hierarchy Seeding: Maps every host to a unique organizational context (Apps, Depts).
    - Time Seeding: Uses Polars to generate a contiguous daily calendar (2023-2025).
    - Logging: Provides a detailed audit trail in logs/00_init_db.log.

What it does:
    - Weighted Seeding (Inventory): Generates hosts using a specific Enterprise Profile.
    - NOTE: Plateau Decline (Black Swan) scenarios are currently disabled in config.py 
      to satisfy MVP requirements and focus on high-probability infrastructure trends.

Outputs:
    - File: horizonscale_synth.db (DuckDB)
    - Metadata: ~2,000 hosts with behavioral "DNA" ready for time-series generation.
"""
"""
00_init_db.py - Database Initialization
Forces all scenario and variant strings to lowercase for project standardization.
"""
"""
00_init_db.py
Author: Sean L Girgis

Purpose:
    Initializes the foundation of the HorizonScale synthetic data pipeline.
    Standardized to force lowercase strings and includes a Success Audit 
    to verify all 10 scenario/variant varieties were created.
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

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)
faker = Faker()

def purge_existing_db(db_path: Path):
    """Purges existing .db files to ensure a clean slate."""
    if db_path.exists():
        db_path.unlink()
        logger.info(f"Clean Slate: Database deleted at {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True) 

def create_schema(con: duckdb.DuckDBPyConnection):
    """Executes DDL to define the database structure."""
    schema_path = SQL_SCHEMA_DIR / "create_tables.sql"
    with open(schema_path, "r") as f:
        con.execute(f.read())
    logger.info("Schema created successfully.")

def seed_hosts_with_variants(con: duckdb.DuckDBPyConnection, num_hosts: int):
    """Generates hosts with behavioral 'DNA' using standardized lowercase strings."""
    logger.info(f"Seeding {num_hosts} hosts with standardized lowercase logic...")
    scenarios = list(SCENARIO_DISTRIBUTION.keys())
    scenario_probs = list(SCENARIO_DISTRIBUTION.values())
    variant_types = list(VARIANT_WEIGHTS.keys())
    variant_probs = list(VARIANT_WEIGHTS.values())
    
    hosts_data = []
    for _ in range(num_hosts):
        uuid_str = faker.uuid4()
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
            "scenario": scenario_member.value,  # Uses 'steady_growth'
            "variant": chosen_variant.lower()    # Uses 'normal'
        })

    hosts_df = pl.DataFrame(hosts_data)
    con.register("hosts_temp", hosts_df)
    con.execute("INSERT INTO hosts SELECT * FROM hosts_temp")
    return [h['node_name'] for h in hosts_data]

def validate_seeding_success(con: duckdb.DuckDBPyConnection):
    """
    SUCCESS CRITERIA AUDIT:
    Verifies that all 10 behavioral varieties were successfully seeded.
    """
    logger.info("--- Validating Database Exit Criteria ---")
    
    # Query to count distribution of Scenario + Variant
    audit_df = con.execute("""
        SELECT scenario, variant, count(*) as count
        FROM hosts
        GROUP BY scenario, variant
        ORDER BY scenario, variant
    """).pl()
    
    total_varieties = len(audit_df)
    total_hosts = audit_df["count"].sum()
    
    # Format a clean summary table for logs and console
    summary = (
        f"\n{'='*60}\n"
        f" SUCCESS: Database Initialization Complete\n"
        f"{'-'*60}\n"
        f" Total Hosts Created: {total_hosts:,}\n"
        f" Behavioral Varieties: {total_varieties} / 10\n"
        f"{'-'*60}\n"
        f"{audit_df}\n"
        f"{'='*60}\n"
    )
    
    if total_varieties < 10:
        logger.warning(f"INCOMPLETE: Only found {total_varieties} varieties. Check SCENARIO_DISTRIBUTION.")
    else:
        logger.info("EXIT CRITERIA SATISFIED: All 10 behavioral varieties are present.")
        
    print(summary)

def seed_business_hierarchy(con: duckdb.DuckDBPyConnection, node_names: list[str]):
    """Maps hosts to organizational departments and apps."""
    hierarchy_data = [{"host_id": h, "department": random.choice(DEPARTMENTS), 
                      "sub_department": f"{faker.word().capitalize()} Ops",
                      "app_code": f"APP-{faker.lexify('????').upper()}",
                      "app_name": f"{faker.company()} Services"} for h in node_names]
    con.register("hierarchy_temp", pl.DataFrame(hierarchy_data))
    con.execute("INSERT INTO business_hierarchy SELECT * FROM hierarchy_temp")

def seed_time_periods(con: duckdb.DuckDBPyConnection, start: str, end: str):
    """Generates the calendar dimension for the telemetry simulation."""
    df_time = pl.date_range(datetime.strptime(start, "%Y-%m-%d"), 
                            datetime.strptime(end, "%Y-%m-%d"), 
                            interval="1d", eager=True).to_frame("date")
    df_time = df_time.with_columns(pl.col("date").dt.strftime("%Y%m").alias("yearmonth"))
    con.register("time_temp", df_time)
    con.execute("INSERT INTO time_periods SELECT * FROM time_temp")

def init_db():
    """Main orchestration for DB setup."""
    purge_existing_db(DB_PATH)
    with duckdb.connect(str(DB_PATH)) as con:
        create_schema(con)
        node_names = seed_hosts_with_variants(con, DEFAULT_NUM_HOSTS)
        seed_business_hierarchy(con, node_names)
        seed_time_periods(con, TIME_START, TIME_END)
        
        # New Success Audit
        validate_seeding_success(con)
        
    logger.info("Database initialized with standardized lowercase data.")

if __name__ == "__main__":
    init_db()