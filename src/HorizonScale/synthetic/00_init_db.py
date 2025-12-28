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

import duckdb  
import argparse
import polars as pl
import numpy as np
import random
from pathlib import Path
from faker import Faker
from datetime import datetime

# Import project-specific configurations and constants
from horizonscale.lib.config import (  
    DB_PATH, SQL_SCHEMA_DIR, DEFAULT_NUM_HOSTS, TIME_START, TIME_END,
    Scenario, SCENARIO_DISTRIBUTION, SCENARIO_VARIANTS, VARIANT_WEIGHTS,
    CLASSIFICATIONS, DEPARTMENTS, REGIONS, SERVER_TYPES 
)  
from horizonscale.lib.logging import init_root_logging

# Initialize project-standard logging and synthetic data generators
logger = init_root_logging(Path(__file__).stem)
faker = Faker()

def get_args():
    """Parse CLI arguments with fallbacks to config.py defaults."""
    parser = argparse.ArgumentParser(
        description="HorizonScale: Initialize DuckDB with Host Metadata and Time Dimensions."
    )
    parser.add_argument("--num_hosts", type=int, default=DEFAULT_NUM_HOSTS)
    parser.add_argument("--start_date", type=str, default=TIME_START)
    parser.add_argument("--end_date", type=str, default=TIME_END)
    parser.add_argument("--db_path", type=str, default=str(DB_PATH))
    return parser.parse_args()

def purge_existing_db(db_path: Path):
    """Physically removes the database file to ensure a clean simulation start."""
    if db_path.exists():
        try:
            db_path.unlink()
            logger.info(f"Clean Slate: Existing database deleted at {db_path}")
        except Exception as e:
            logger.error(f"Critical: Could not delete database file: {e}")
            raise
    else:
        # Create directory structure if this is the first run
        db_path.parent.mkdir(parents=True, exist_ok=True) 

def create_schema(con: duckdb.DuckDBPyConnection):
    """Loads and executes table definitions from the project's SQL schema directory."""
    schema_path = SQL_SCHEMA_DIR / "create_tables.sql"
    
    if not schema_path.exists():
        logger.error(f"Schema file not found at {schema_path}")
        raise FileNotFoundError(f"Missing schema file: {schema_path}")

    with open(schema_path, "r") as f:
        sql_commands = f.read()
        
    try:
        con.execute(sql_commands)
        logger.info("Database schema created successfully from SQL file.")
    except Exception as e:
        logger.error(f"Failed to execute schema SQL: {e}")
        raise

def seed_hosts_with_variants(con: duckdb.DuckDBPyConnection, num_hosts: int) -> list[str]:
    """
    Generates host metadata using two-layer weighted randomization:
    1. Scenario Layer (e.g., Seasonal vs Burst)
    2. Variant Layer (e.g., Normal vs Breach)
    """
    logger.info(f"Seeding {num_hosts} hosts with weighted Scenario/Variant logic...")
    
    # Extract distribution rules from config
    scenarios = list(SCENARIO_DISTRIBUTION.keys())
    scenario_probs = list(SCENARIO_DISTRIBUTION.values())
    
    variant_types = list(VARIANT_WEIGHTS.keys())
    variant_probs = list(VARIANT_WEIGHTS.values())
    
    hosts_data = []
    
    for _ in range(num_hosts):
        # Generate short, unique enterprise-style node names
        uuid_str = faker.uuid4()
        node_name = f"server-{uuid_str[:8]}"
        
        # Determine the host's behavioral "DNA"
        scenario_member = np.random.choice(scenarios, p=scenario_probs)
        v_type = np.random.choice(variant_types, p=variant_probs)
        chosen_variant = SCENARIO_VARIANTS[scenario_member][v_type]

        # Populate core metadata attributes
        hosts_data.append({
            "node_name": node_name,
            "classification": random.choice(CLASSIFICATIONS),
            "server_type": random.choice(SERVER_TYPES),
            "region": random.choice(REGIONS),
            "cpu_cores": random.choice([16, 32, 64, 128]),
            "memory_gb": random.choice([64, 128, 256, 512]),
            "storage_capacity_mb": random.choice([500000, 1000000, 2000000, 5000000]),
            "department": random.choice(DEPARTMENTS),
            "scenario": scenario_member.name,  # Stores Enum key for logic mapping
            "variant": chosen_variant          # Stores string for param mapping
        })

    # Use Polars for high-speed bulk insertion into DuckDB
    hosts_df = pl.DataFrame(hosts_data)
    con.register("hosts_temp", hosts_df)
    con.execute("INSERT INTO hosts SELECT * FROM hosts_temp")
    
    logger.info(f"Successfully seeded {num_hosts} hosts into inventory.")
    
    # Return names to ensure referential integrity in child tables
    return [h['node_name'] for h in hosts_data]

def seed_business_hierarchy(con: duckdb.DuckDBPyConnection, node_names: list[str]) -> None:
    """Creates organizational mapping for every host in the inventory."""
    hierarchy_data = []
    
    for host_id in node_names:
        hierarchy_data.append({
            "host_id": host_id,
            "department": faker.random_element(elements=DEPARTMENTS),
            "sub_department": f"{faker.word().capitalize()} Operations",
            "app_code": faker.lexify(text="APP-????").upper(),
            "app_name": f"{faker.company()} Services"
        })

    hierarchy_df = pl.DataFrame(hierarchy_data)
    con.register("hierarchy_temp", hierarchy_df)
    con.execute("INSERT INTO business_hierarchy SELECT * FROM hierarchy_temp")
    logger.info("Business hierarchy seeded.")

def seed_time_periods(con: duckdb.DuckDBPyConnection, start_date: str, end_date: str) -> None:
    """Generates a daily calendar dimension for the simulation window."""
    logger.info(f"Seeding time_periods from {start_date} to {end_date}...")

    # Instant daily date generation via Polars
    df_time = pl.date_range(
        start=datetime.strptime(start_date, "%Y-%m-%d"),
        end=datetime.strptime(end_date, "%Y-%m-%d"),
        interval="1d",
        eager=True
    ).to_frame("date")

    # Add YearMonth column for efficient partitioning/grouping (e.g., 202512)
    df_time = df_time.with_columns(
        pl.col("date").dt.strftime("%Y%m").alias("yearmonth")
    )

    con.register("time_temp", df_time)
    con.execute("INSERT INTO time_periods SELECT * FROM time_temp")
    logger.info(f"Seeded {len(df_time)} daily time periods.")    
    
def validate_exit_criteria(con: duckdb.DuckDBPyConnection, expected_hosts: int) -> None:
    """
    Exit Criteria: Validates the state of the database before script termination.
    Uses 'ASCII_FULL' to ensure compatibility with Windows terminal encodings.
    """
    logger.info("--- Validating Script Exit Criteria ---")

    # 1. Fetch exact counts from the new DB
    actual_hosts = con.execute("SELECT COUNT(*) FROM hosts").fetchone()[0]
    actual_hierarchy = con.execute("SELECT COUNT(*) FROM business_hierarchy").fetchone()[0]
    actual_days = con.execute("SELECT COUNT(*) FROM time_periods").fetchone()[0]

    # 2. Hard Assertion (Technical Exit Criteria)
    if actual_hosts != expected_hosts:
        logger.error(f"FAIL: Host count mismatch! Expected {expected_hosts}, found {actual_hosts}")
        raise ValueError("Exit criteria failed: Database integrity check failed.")

    # 3. Generate Scenario/Variant Distribution Stats
    stats_query = """
        SELECT 
            scenario, 
            variant, 
            COUNT(*) as count,
            round(count * 100.0 / sum(count) over(), 2) as pct
        FROM hosts
        GROUP BY 1, 2
        ORDER BY count DESC
    """
    stats_df = con.execute(stats_query).pl()

    # 4. Format for Terminal (Using ASCII_FULL for Windows compatibility)
    # This prevents the 'charmap' codec error while keeping a clear table structure
    with pl.Config(
        tbl_formatting="ASCII_FULL", 
        tbl_hide_column_data_types=True, 
        tbl_rows=20
    ):
        stats_table = str(stats_df)

    # 5. Professional Logging Output
    logger.info("Database seeding successfully verified.")
    logger.info(f"Final Inventory: {actual_hosts} Hosts | {actual_hierarchy} Hierarchy | {actual_days} Days")
    logger.info(f"\nScenario/Variant Distribution:\n{stats_table}")

    # 6. High-Visibility Terminal Summary
    summary = (
        f"\n{'='*60}\n"
        f" SUCCESS: 00_init_db Genesis Complete\n"
        f"{'-'*60}\n"
        f" DB Path:      {DB_PATH}\n"
        f" Total Hosts:  {actual_hosts}\n"
        f" Time Window:  {actual_days} Days ({TIME_START} to {TIME_END})\n"
        f"{'='*60}\n"
    )
    print(summary)       
    
def init_db(num_hosts: int, db_str_path: str, start_date: str, end_date: str):
    """Main execution flow for database initialization."""
    db_path = Path(db_str_path)
    
    logger.info(f"Initializing simulation environment | Target: {num_hosts} hosts")
    
    # Clean up previous runs
    purge_existing_db(db_path)
    
    # Establish connection using context manager for safe auto-close
    with duckdb.connect(str(db_path)) as con:
        # 1. Build table structures
        create_schema(con)
        
        # 2. Seed host inventory (captures names for referential integrity)
        node_names = seed_hosts_with_variants(con, num_hosts)

        # 3. Seed organizational hierarchy linked to hosts
        seed_business_hierarchy(con, node_names)        
    
        # 4. Seed time dimension
        seed_time_periods(con, start_date, end_date) 
        # --- NEW EXIT CRITERIA CALL ---
        validate_exit_criteria(con, num_hosts)



    logger.info("Genesis complete: Database is ready for data generation.")

if __name__ == "__main__":  
    args = get_args()
    init_db(
        num_hosts=args.num_hosts, 
        db_str_path=args.db_path, 
        start_date=args.start_date, 
        end_date=args.end_date
    )