# HorizonScale – Personal Portfolio Project  
**Official Project Charter & Execution Plan**  
Goal: Build a 100% original, production-grade, resume-defining capacity planning & forecasting system from scratch, inspired by (but never copying) an existing internal tool you have access to.

**Project Owner:** Sean L Girgis  

**Target Completion:** 3–4 weeks (fast but extremely high quality)  
**Outcome:** Public GitHub repository + 12–18 page technical paper + killer resume bullets + interview-ready talking points

### Phase B: Simulation & Portfolio System Creation  
**Sub-Task 3: Data Pipeline & Forecasting – Explanation of data_pipeline.py**  

**Author:** Sean L Girgis  
**Date:** December 21, 2025  

The `data_pipeline.py` script is the consolidation step in Phase B – it loads all the generated monthly resource CSVs, merges them into a single unified time-series DataFrame, enriches with business hierarchies and scenario_type from the DB, and saves as a Parquet file for efficient downstream analysis and forecasting.

### What the Script Does
- It accepts `--years` argument (default [2023, 2024, 2025]) to select the data range.
- Loops over all generated CSVs in `generated_csvs/YYYY/MM/` and reads them with Polars.
- Concats all DFs diagonally (handles different columns from different resources).
- Sorts by node_name and date for time-series order.
- Uses DuckDB to join business_hierarchy and hosts tables from the SQLite DB for enrichment (adds app_manager/business_owner fields and scenario_type).
- Saves the combined DF as `combined_time_series.parquet` in `processed/`.

The script is modular and can be run in background if needed (add --background option later).

### What It Uses
- `polars`: For reading CSVs, concat, sort, and writing Parquet.
- `duckdb`: For connecting to SQLite DB and executing SQL join query.
- `argparse`: For command-line args (--years).
- `pathlib`: For path handling.
- Imports from `horizonscale.config`: SYNTHETIC_DATA_DIR, DB_PATH, PROCESSED_DATA_DIR.

### How It Fits the Project
This script bridges the synthetic generation to the forecasting pipeline: CSVs (mimicking original inputs) are merged into one efficient Parquet for EDA, preprocessing, and modeling. It enables fast loading of 6M+ rows for ML without re-reading 144 CSVs every time. It fits after `generate_csv_inputs.py` (which creates the CSVs) and before notebooks for EDA/preprocessing/forecasting.

### Pipeline Flow & Order
1. `generate_synthetic_data.py`: Seeds the DB with hosts, hierarchies, time periods.
2. `generate_csv_inputs.py`: Generates resource CSVs (CPU/vCPU/MEM/STO) from DB with scenarios.
3. `data_pipeline.py`: Merges all CSVs, enriches from DB, saves combined Parquet.
4. `notebooks/eda_and_preprocessing.ipynb`: Loads Parquet, EDA, preprocess, saves preprocessed Parquet.
5. `notebooks/baseline_forecasting.ipynb`: Loads preprocessed, runs Prophet baseline.
6. `notebooks/advanced_forecasting_darts.ipynb`: Loads preprocessed, runs TFT advanced forecast.
7. `app.py`: Streamlit dashboard loads Parquet/models for visualization.

### Suggested Doc Note
**File Name**: `docs/phase_b_3_data_pipeline_explanation.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_3_data_pipeline_explanation.md
git commit -m "Document data_pipeline.py functionality and pipeline order"
git push
```

Ready for Streamlit dashboard or paper? Let me know. We're in the home stretch! 🚀

```python
import polars as pl
import duckdb
import argparse
from pathlib import Path
from horizonscale.config import SYNTHETIC_DATA_DIR, DB_PATH, PROCESSED_DATA_DIR

def load_and_merge_data(years: list = [2023, 2024, 2025]):
    """Load all resource CSVs and merge into a unified time-series DF."""
    data_dir = Path(SYNTHETIC_DATA_DIR)
    dfs = []
    
    for year in years:
        year_dir = data_dir / str(year)
        if not year_dir.exists():
            print(f"No data for {year}")
            continue
        
        for month_dir in year_dir.iterdir():
            if month_dir.is_dir():
                for csv_file in month_dir.glob("*.csv"):
                    df = pl.read_csv(csv_file)
                    dfs.append(df)
    
    if not dfs:
        print("No CSVs found.")
        return None
    
    combined = pl.concat(dfs, how="diagonal")
    
    # Sort by node_name, date
    combined = combined.sort(["node_name", "date"])
    
    # Enrich with hosts (scenario_type) and hierarchy using DuckDB
    db_str = str(DB_PATH)
    con = duckdb.connect()
    con.execute(f"ATTACH '{db_str}' AS sqlite_db (TYPE SQLITE);")
    con.execute("USE sqlite_db;")
    
    query = """
    SELECT combined.*, h.*, hosts.scenario_type
    FROM combined
    LEFT JOIN business_hierarchy h ON combined.node_name = h.node_name
    LEFT JOIN hosts ON combined.node_name = hosts.node_name
    """
    enriched = con.execute(query).pl()
    con.close()
    
    output_path = PROCESSED_DATA_DIR / "combined_time_series.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.write_parquet(output_path)
    print(f"Combined data saved: {output_path} with {len(enriched)} rows")
    
    return enriched

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Pipeline: Load & Merge CSVs")
    parser.add_argument("--years", nargs="+", type=int, default=[2023, 2024, 2025], help="Years to load")
    args = parser.parse_args()
    
    load_and_merge_data(args.years)
```