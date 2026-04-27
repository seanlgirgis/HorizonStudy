# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 1: Synthetic Data Generator – Explanation of generate_synthetic_data.py**  

**Author:** Sean L Girgis  
**Date:** December 21, 2025  

The `generate_synthetic_data.py` script is the core of Phase B – it creates the realistic synthetic data we use for the entire project.

### What the Script Does
It initializes a SQLite database (`horizonscale_synth.db`) with three tables:
- `hosts`: Stores server details (node_name, classification (physical/virtual), server_type (Unix/Linux/Windows/Other), region, max_cores, memory_gb, storage_capacity_mb, scenario_type).
- `business_hierarchy`: Adds ownership/enrichment data linked by node_name (app_manager and business_owner fields for alerting).
- `time_periods`: Daily dates from 2023-01-01 to 2025-12-01 (for time-series).

It seeds the tables:
- Generates 2500 hosts (configurable) with even distribution across classification/server_type combos.
- Assigns random but realistic capacities (power-of-2 cores/memory).
- Assigns scenario_type (steady_growth, seasonal, burst, low_idle, capacity_breach, plateau_decline) evenly.
- Adds fake business hierarchies using Faker for realistic names/IDs.
- Populates time periods for daily rows.

The script is idempotent (OR IGNORE inserts) so re-running is safe.

### What It Uses
- `sqlite3`: For DB connection and SQL execution.
- `polars`: To generate time range (date_range).
- `faker`: For realistic node names, regions, hierarchies.
- `random`: For random choices (regions, cores, etc.).
- `datetime`: For date parsing.
- `argparse`: For --num_hosts argument.
- Imports from `horizonscale.config`: DB_PATH, SQL_SCHEMA_DIR, REGIONS, CLASSIFICATIONS, SERVER_TYPES, DEPARTMENTS, TIME_START, TIME_END, TIME_INTERVAL, DEFAULT_NUM_HOSTS, FIELD_*, SCENARIOS, BASE_UTIL, FIELD_SCENARIO_TYPE, GROWTH_RATE, BURST_PROB.

### How It Fits the Project
This script creates the master seed data. The CSV exporters then use it to generate resource files (CPU/vCPU/MEM/STO) with scenario-based patterns. The pipeline merges them into Parquet for forecasting.

### Suggested Doc Note
**File Name**: `docs/phase_b_1_generate_synthetic_data_explanation.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_1_generate_synthetic_data_explanation.md
git commit -m "Document generate_synthetic_data.py functionality"
git push
```

Ready for dashboard or paper? Let me know what you want next. We're in the home stretch! 🚀


```python
import sqlite3
import polars as pl
from faker import Faker
from datetime import datetime
import argparse
import random
from horizonscale.config import (
    DB_PATH, SQL_SCHEMA_DIR, REGIONS, CLASSIFICATIONS, SERVER_TYPES, DEPARTMENTS,
    TIME_START, TIME_END, TIME_INTERVAL, DEFAULT_NUM_HOSTS
)

def generate_id(name: str) -> str:
    """Generate realistic ID: First initial + Last initial + 6 random digits."""
    parts = name.split()
    if len(parts) < 2:
        return f"{name[0].upper()}{name[-1].upper()}{random.randint(100000, 999999)}"
    first_init = parts[0][0].upper()
    last_init = parts[-1][0].upper()
    random_digits = str(random.randint(100000, 999999))
    return f"{first_init}{last_init}{random_digits}"

def init_db(db_path: str, num_hosts: int):
    """Initialize SQLite DB: Create tables, seed hosts with classification/server_type/scenario_type, hierarchies, time periods."""
    conn = sqlite3.connect(db_path)
    
    # Run schema creation
    schema_path = SQL_SCHEMA_DIR / "01_create_tables.sql"
    try:
        with open(schema_path) as f:
            conn.executescript(f.read())
    except FileNotFoundError:
        print(f"Error: Ensure '{schema_path}' exists.")
        return
    
    fake = Faker('en_US')  # Use American English locale for realistic names
    
    # Calculate even distribution: ~equal hosts per combo
    num_combos = len(CLASSIFICATIONS) * len(SERVER_TYPES)
    hosts_per_combo = num_hosts // num_combos
    
    # Define scenarios
    scenarios = ['steady_growth', 'seasonal', 'burst', 'low_idle', 'capacity_breach', 'plateau_decline']
    
    hosts_data = []
    for cls in CLASSIFICATIONS:
        for stype in SERVER_TYPES:
            for _ in range(hosts_per_combo):
                node_name = f"server-{fake.uuid4()[:8]}"
                region = fake.random_element(REGIONS)
                
                # Practical max_cores: powers of 2, higher for physical
                if cls == 'physical':
                    cores_options = [16, 32, 64]
                else:
                    cores_options = [2, 4, 8, 16]
                max_cores = fake.random_element(cores_options)
                
                # Practical memory_gb: powers of 2, scaled to cores
                memory_options = [4, 8, 16, 32, 64, 128, 256, 512]
                memory_gb = fake.random_element(memory_options)
                
                storage_mb = fake.random_int(500000, 5000000)  # 500GB–5TB
                
                # Assign scenario evenly or randomly
                scenario_type = fake.random_element(scenarios)
                
                hosts_data.append((node_name, cls, stype, region, max_cores, memory_gb, storage_mb, scenario_type))
    
    conn.executemany("INSERT OR IGNORE INTO hosts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", hosts_data)
    
    # Insert hierarchies (distinct IDs, names, and depts for app_manager and business_owner)
    hierarchy_data = []
    for node_name, _, _, _, _, _, _, _ in hosts_data:
        # App Manager: Separate name and IDs
        app_name = fake.name()
        app_goc = generate_id(app_name)
        app_le_id = generate_id(app_name)
        app_le_name = app_name
        app_ms_id = generate_id(app_name)
        app_ms_name = fake.random_element(DEPARTMENTS)
        
        # Business Owner: Completely separate name and IDs
        business_name = fake.name()
        business_goc = generate_id(business_name)
        business_le_id = generate_id(business_name)
        business_le_name = business_name
        business_ms_id = generate_id(business_name)
        business_ms_name = fake.random_element(DEPARTMENTS)
        
        hierarchy_data.append((
            node_name,
            app_goc, app_le_id, app_le_name,
            app_ms_id, app_ms_name,
            business_goc, business_le_id, business_le_name,
            business_ms_id, business_ms_name,
            'NO'
        ))
    
    conn.executemany("""
        INSERT OR IGNORE INTO business_hierarchy 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, hierarchy_data)
    
    # Generate time periods
    start_date = datetime.strptime(TIME_START, '%Y-%m-%d')
    end_date = datetime.strptime(TIME_END, '%Y-%m-%d')
    dates = pl.date_range(
        start=pl.lit(start_date),
        end=pl.lit(end_date),
        interval=TIME_INTERVAL,
        eager=True
    ).to_list()
    
    time_data = [(d.strftime('%Y-%m-%d'), d.strftime('%Y%m')) for d in dates]
    conn.executemany("INSERT OR IGNORE INTO time_periods VALUES (?, ?)", time_data)
    
    conn.commit()
    conn.close()
    
    print(f"DB initialized at {db_path}")
    print(f"- {len(hosts_data)} hosts seeded ({len(CLASSIFICATIONS) * len(SERVER_TYPES)} combinations)")
    print(f"- {len(time_data)} time periods added")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Data Generator")
    parser.add_argument("--num_hosts", type=int, default=DEFAULT_NUM_HOSTS, help="Total number of hosts (distributed across combinations)")
    args = parser.parse_args()
    
    init_db(DB_PATH, args.num_hosts)
```