# HorizonScale – Personal Portfolio Project  
**Official Project Charter & Execution Plan**  
Goal: Build a 100% original, production-grade, resume-defining capacity planning & forecasting system from scratch, inspired by (but never copying) an existing internal tool you have access to.

**Project Owner:** Sean L Girgis  

**Target Completion:** 3–4 weeks (fast but extremely high quality)  
**Outcome:** Public GitHub repository + 12–18 page technical paper + killer resume bullets + interview-ready talking points

### Phase B: Simulation & Portfolio System Creation  
**Sub-Task 1: Synthetic Data Generator – Add Scenario Type to Hosts Table**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

To enable diverse scenarios in time-series data, the `hosts` table has been updated to include a new column `scenario_type` (e.g., 'steady_growth', 'seasonal', 'burst', 'low_idle', 'capacity_breach', 'plateau_decline'). This is assigned evenly across hosts during seeding.

### Updated `generate_synthetic_data.py` (Full File)
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

### Updated `data/synthetic/sql/01_create_tables.sql` (Full File)
```sql
-- HorizonScale Synthetic Data Schema v1.0
-- Author: Sean L Girgis
-- Date: December 20, 2025

DROP TABLE IF EXISTS business_hierarchy;
DROP TABLE IF EXISTS time_periods;
DROP TABLE IF EXISTS hosts;

-- Hosts table with scenario_type
CREATE TABLE hosts (
    node_name TEXT PRIMARY KEY,
    classification TEXT NOT NULL,  -- physical or virtual
    server_type TEXT NOT NULL,     -- Unix, Linux, Windows, Other
    region TEXT NOT NULL,
    max_cores INTEGER,
    memory_gb INTEGER,
    storage_capacity_mb REAL,
    scenario_type TEXT             -- e.g., steady_growth, seasonal
);

CREATE TABLE business_hierarchy (
    node_name TEXT NOT NULL,
    app_manager_goc TEXT,
    app_manager_le_id TEXT,
    app_manager_le_name TEXT,
    app_manager_ms_id TEXT,
    app_manager_ms_name TEXT,
    business_owner_goc TEXT,
    business_owner_le_id TEXT,
    business_owner_le_name TEXT,
    business_owner_ms_id TEXT,
    business_owner_ms_name TEXT,
    suppress_alerts TEXT DEFAULT 'NO',
    FOREIGN KEY (node_name) REFERENCES hosts (node_name)
);

CREATE TABLE time_periods (
    date DATE NOT NULL,
    yearmonth TEXT NOT NULL,  -- YYYYMM
    PRIMARY KEY (date)
);

-- Indexes for performance
CREATE INDEX idx_hosts_region ON hosts(region);
CREATE INDEX idx_hierarchy_node ON business_hierarchy(node_name);
```

### Updated `data/synthetic/sql/02_insert_master_data.sql` (Full File)
Since 02_insert_master_data.sql was a duplicate of 01, I've updated it to match the new schema. If it's not needed, you can delete it or use it for insert examples.

```sql
-- HorizonScale Synthetic Data Schema v1.0
-- Author: Sean L Girgis
-- Date: December 20, 2025

DROP TABLE IF EXISTS business_hierarchy;
DROP TABLE IF EXISTS time_periods;
DROP TABLE IF EXISTS hosts;

-- Hosts table with scenario_type
CREATE TABLE hosts (
    node_name TEXT PRIMARY KEY,
    classification TEXT NOT NULL,  -- physical or virtual
    server_type TEXT NOT NULL,     -- Unix, Linux, Windows, Other
    region TEXT NOT NULL,
    max_cores INTEGER,
    memory_gb INTEGER,
    storage_capacity_mb REAL,
    scenario_type TEXT             -- e.g., steady_growth, seasonal
);

CREATE TABLE business_hierarchy (
    node_name TEXT NOT NULL,
    app_manager_goc TEXT,
    app_manager_le_id TEXT,
    app_manager_le_name TEXT,
    app_manager_ms_id TEXT,
    app_manager_ms_name TEXT,
    business_owner_goc TEXT,
    business_owner_le_id TEXT,
    business_owner_le_name TEXT,
    business_owner_ms_id TEXT,
    business_owner_ms_name TEXT,
    suppress_alerts TEXT DEFAULT 'NO',
    FOREIGN KEY (node_name) REFERENCES hosts (node_name)
);

CREATE TABLE time_periods (
    date DATE NOT NULL,
    yearmonth TEXT NOT NULL,  -- YYYYMM
    PRIMARY KEY (date)
);

-- Indexes for performance
CREATE INDEX idx_hosts_region ON hosts(region);
CREATE INDEX idx_hierarchy_node ON business_hierarchy(node_name);
```

### Action Steps
1. **Overwrite Files**:  
   - Paste the updated `generate_synthetic_data.py`.  
   - Update `01_create_tables.sql` and `02_insert_master_data.sql`.  

2. **Re-run Seeding**:  
   ```powershell
   python -m horizonscale.generate_synthetic_data --num_hosts 8000
   ```

3. **Verify**:  
   - `sqlite3 data/synthetic/horizonscale_synth.db "SELECT scenario_type, COUNT(*) FROM hosts GROUP BY scenario_type;"` – Expect even distribution.

4. **Commit**:  
   ```powershell
   git add src/horizonscale/generate_synthetic_data.py data/synthetic/sql/01_create_tables.sql data/synthetic/sql/02_insert_master_data.sql
   git commit -m "Add scenario_type to hosts table for diverse data patterns"
   git push
   ```

### Suggested Doc Note
**File Name**: `docs/phase_b_1_synthetic_data_scenario_addition.md`  
**Content**: Paste this response.

This enables scenarios in future files. Ready for the updated CSV exporter with scenario-based util generation? Let me know. 🚀