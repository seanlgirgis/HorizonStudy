# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 2: CSV File Generation – Explanation of generate_csv_inputs.py**  

**Author:** Sean L Girgis  
**Date:** December 21, 2025  

The `generate_csv_inputs.py` script takes the master seed data from the SQLite DB (hosts with scenario_type, time periods) and generates the monthly resource CSVs (CPU, vCPU, MEM, STO) that mimic the original Trenda inputs.

### What the Script Does
- It accepts arguments like `--resource cpu` (or vcpu/mem/sto) and `--year 2024` (or `--month OCT2024` for single).
- Queries the DB for hosts (filtered by classification for CPU/vCPU) and time periods.
- For each host and day in the month/year, generates P95 utilization values based on the host's `scenario_type` (steady_growth, seasonal, burst, low_idle, capacity_breach, plateau_decline) using NumPy formulas (linspace for trends, sin for seasonality, random bursts, etc.).
- Adds noise and caps at 0–100%.
- Outputs monthly CSVs in `data/synthetic/generated_csvs/YYYY/MM/` (e.g., `cpu_p95_OCT2024.csv`).

### What It Uses
- `sqlite3`: DB connection.
- `polars`: Reading DB queries.
- `numpy`: Generating util series (linspace, sin, random).
- `argparse`: Command-line args.
- `os`: Directory creation.
- Imports from `horizonscale.config`: DB_PATH, TABLE_HOSTS, TABLE_TIME_PERIODS, SYNTHETIC_DATA_DIR, FIELD_*, SCENARIOS, BASE_UTIL, GROWTH_RATE, BURST_PROB.

### Pipeline Flow & Order
1. **generate_synthetic_data.py**: Runs first – creates the seed DB with hosts, hierarchies, time periods, and assigns scenario_type.
2. **generate_csv_inputs.py**: Uses the DB to generate CSVs for each resource/month/year (e.g., run for CPU 2023-2025, then vCPU, MEM, STO).
3. **data_pipeline.py**: Loads all CSVs, merges into one DF, enriches with DB hierarchies, saves combined Parquet.
4. **notebooks/eda_and_preprocessing.ipynb**: Loads Parquet, does EDA, filters, interpolates, rollup, saves preprocessed Parquet.
5. **notebooks/baseline_forecasting.ipynb**: Loads preprocessed, runs Prophet baseline forecast.
6. **notebooks/advanced_forecasting_darts.ipynb**: Loads preprocessed, runs TFT advanced forecast.

The flow is sequential: seed DB → CSVs → merged Parquet → preprocessed Parquet → forecasting notebooks → dashboard.

### Suggested Doc Note
**File Name**: `docs/phase_b_2_generate_csv_inputs_explanation.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_2_generate_csv_inputs_explanation.md
git commit -m "Document generate_csv_inputs.py functionality and pipeline order"
git push
```

Ready for dashboard or paper? Let me know what you want next. We're in the home stretch! 🚀

```python
import sqlite3
import polars as pl
import argparse
import os
import numpy as np
from horizonscale.config import (
    DB_PATH, TABLE_HOSTS, TABLE_TIME_PERIODS, SYNTHETIC_DATA_DIR,
    FIELD_NODE_NAME, FIELD_REGION, FIELD_MAX_CORES, FIELD_MEMORY_GB, FIELD_STORAGE_MB,
    FIELD_CLASSIFICATION, FIELD_SCENARIO_TYPE, SCENARIOS, BASE_UTIL
)
from datetime import datetime

def generate_util_series(scenario: str, num_days: int, max_cap: int = 100):
    """Generate P95 utilization series based on scenario_type."""
    base = BASE_UTIL.get(scenario, 50.0)
    series = np.full(num_days, base)
    
    if scenario == 'steady_growth':
        series += np.linspace(0, 30, num_days)
    elif scenario == 'seasonal':
        weekly = 20 * np.sin(2 * np.pi * np.arange(num_days) / 7)
        yearly = 15 * np.sin(2 * np.pi * np.arange(num_days) / 365)
        series += weekly + yearly
    elif scenario == 'burst':
        bursts = np.random.choice([0, 60], num_days, p=[0.98, 0.02])
        series += bursts
    elif scenario == 'low_idle':
        series = np.full(num_days, 10.0) + np.random.normal(0, 5, num_days)
    elif scenario == 'capacity_breach':
        series += np.linspace(0, 80, num_days)
    elif scenario == 'plateau_decline':
        series -= np.linspace(0, 40, num_days)
    
    series += np.random.normal(0, 5, num_days)
    return np.clip(series, 0, 100)

def generate_year_csv(resource: str, year: str):
    """Generate CSVs for all 12 months in a year."""
    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    for month in months:
        generate_monthly_csv(resource, f"{month}{year}")

def generate_monthly_csv(resource: str, month: str):
    """Generate single month CSV for given resource."""
    year = month[-4:]
    month_num = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
        "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
    }[month[:3].upper()]
    yearmonth = f"{year}{month_num}"
    
    conn = sqlite3.connect(DB_PATH)
    
    # Host query based on resource
    if resource == "cpu":
        query = f"SELECT {FIELD_NODE_NAME}, {FIELD_REGION}, {FIELD_MAX_CORES}, {FIELD_SCENARIO_TYPE} FROM {TABLE_HOSTS} WHERE {FIELD_CLASSIFICATION} = 'physical'"
    elif resource == "vcpu":
        query = f"SELECT {FIELD_NODE_NAME}, {FIELD_REGION}, {FIELD_MAX_CORES}, {FIELD_SCENARIO_TYPE} FROM {TABLE_HOSTS} WHERE {FIELD_CLASSIFICATION} = 'virtual'"
    elif resource == "mem":
        query = f"SELECT {FIELD_NODE_NAME}, {FIELD_REGION}, {FIELD_MAX_CORES}, {FIELD_MEMORY_GB}, {FIELD_SCENARIO_TYPE} FROM {TABLE_HOSTS}"
    elif resource == "sto":
        query = f"SELECT {FIELD_NODE_NAME}, {FIELD_REGION}, {FIELD_STORAGE_MB}, {FIELD_SCENARIO_TYPE} FROM {TABLE_HOSTS}"
    
    hosts_df = pl.read_database(query=query, connection=conn)
    
    # Time periods for month
    time_df = pl.read_database(
        query=f"SELECT date, yearmonth FROM {TABLE_TIME_PERIODS} WHERE yearmonth = '{yearmonth}'",
        connection=conn
    )
    
    conn.close()
    
    if time_df.is_empty():
        print(f"No data for yearmonth {yearmonth}.")
        return
    
    data = []
    for host in hosts_df.iter_rows(named=True):
        scenario = host[FIELD_SCENARIO_TYPE]
        util_series = generate_util_series(scenario, len(time_df))
        
        for i, time in enumerate(time_df.iter_rows(named=True)):
            row = {
                "node_name": host[FIELD_NODE_NAME],
                "date": time['date'],
                "yearmonth": time['yearmonth'],
                "region": host[FIELD_REGION]
            }
            if resource == "cpu":
                row["cpu_p95"] = util_series[i]
                row["max_cores"] = host[FIELD_MAX_CORES]
            elif resource == "vcpu":
                row["vcpu_p95"] = util_series[i]
                row["max_cores"] = host[FIELD_MAX_CORES]
            elif resource == "mem":
                row["p95resmem_util"] = util_series[i]
                row["memory_gb"] = host[FIELD_MEMORY_GB]
            elif resource == "sto":
                row["P95_Storage_Util_Pct"] = util_series[i]
                row["Storage_Capacity_mb"] = host[FIELD_STORAGE_MB]
            
            data.append(row)
    
    df = pl.DataFrame(data)
    output_dir = SYNTHETIC_DATA_DIR / year / month_num
    os.makedirs(output_dir, exist_ok=True)
    output_path = output_dir / f"{resource}_p95_{month.upper()}.csv"
    df.write_csv(output_path)
    
    print(f"Generated: {output_path} with {len(df)} rows")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Synthetic CSV Inputs")
    parser.add_argument("--resource", required=True, choices=["cpu", "vcpu", "mem", "sto"], help="Resource type")
    parser.add_argument("--year", default="2024", help="Year to generate (default: 2024)")
    args = parser.parse_args()
    
    generate_year_csv(args.resource, args.year)

    ```