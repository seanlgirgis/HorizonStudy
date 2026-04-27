# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 3: Data Pipeline & Forecasting – Next Phase**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

With three years of data generated (2023–2025) and 4 files per month (CPU, vCPU, MEM, STO) committed and pushed to GitHub, the synthetic input data is complete. The repository is clean, and we're ready to move forward.

### Current Status
- Synthetic CSVs: 36 months × 4 files = 144 CSVs in `data/synthetic/generated_csvs/`.
- Repo: All code pushed, no generated data committed (via .gitignore).
- Ready for: Loading data into Polars + DuckDB, preprocessing, and running forecasts.

### Next Step: Build the Data Pipeline
We'll create a script to load all CSVs, merge into a unified time-series DF, and prepare for modeling.

**New Script**: `src/horizonscale/data_pipeline.py`  
This loads CSVs, combines resources, adds hierarchies from DB, and outputs a Parquet file for efficiency.

**Full Code for `src/horizonscale/data_pipeline.py`**:
```python
import polars as pl
import argparse
from pathlib import Path
from horizonscale.config import SYNTHETIC_DATA_DIR, DB_PATH

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
    
    # Add hierarchies from DB
    conn = pl.connect(DB_PATH)
    hierarchy = pl.read_database("SELECT * FROM business_hierarchy", conn)
    conn.close()
    
    combined = combined.join(hierarchy, on="node_name", how="left")
    
    output_path = data_dir / "combined_time_series.parquet"
    combined.write_parquet(output_path)
    print(f"Combined data saved: {output_path} with {len(combined)} rows")
    
    return combined

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Pipeline: Load & Merge CSVs")
    parser.add_argument("--years", nargs="+", type=int, default=[2023, 2024, 2025], help="Years to load")
    args = parser.parse_args()
    
    load_and_merge_data(args.years)
```

### Action Steps
1. **Create the Script**:
   ```powershell
   notepad src\horizonscale\data_pipeline.py
   ```
   - Paste the code.

2. **Run**:
   ```powershell
   python -m horizonscale.data_pipeline --years 2023 2024 2025
   ```

3. **Verify**:
   - Check `data/synthetic/combined_time_series.parquet` exists.
   - Run in notebook: `pl.read_parquet(...)` to inspect.

4. **Commit**:
   ```powershell
   git add src/horizonscale/data_pipeline.py
   git commit -m "Add data pipeline to load and merge all resource CSVs"
   git push
   ```

### Suggested Doc Note
**File Name**: `docs/phase_b_3_data_pipeline.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_3_data_pipeline.md
git commit -m "Document data pipeline step"
git push
```

This creates a unified dataset. Next: Preprocessing and baseline Prophet forecast on CPU. Ready? Let me know. 🚀