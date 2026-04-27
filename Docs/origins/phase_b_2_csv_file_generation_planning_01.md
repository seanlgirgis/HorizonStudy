# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 2: CSV File Generation – Baby Steps: First File Type**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

Excellent plan – let's take it one step at a time. We'll start with the simplest file type: **CPU Metrics** (`cpu_p95_OCT2024.csv`). This will be the earliest month in our synthetic data range (October 2024, as the time_periods table covers 2023-01-01 to 2025-12-01).

### Why Start with CPU Metrics?
- Simplest structure: Uses physical hosts only (classification = 'physical').
- Key columns: node_name, date, cpu_p95 (P95 utilization), max_cores, yearmonth, region.
- Allows us to test DB query + time-series generation + CSV export before adding complexity (vCPU filtering, bursts, etc.).

### File Details
- **File Name**: `cpu_p95_OCT2024.csv`
- **Output Path**: `data/synthetic/generated_csvs/2024/10/cpu_p95_OCT2024.csv`
- **Content**:  
  - One row per day per physical host in October 2024.  
  - `cpu_p95`: Generated as 30–80% baseline + light noise (no scenarios yet).  
  - `yearmonth`: "202410"  
  - `date`: YYYY-MM-DD  
  - `max_cores`: From hosts table  
  - `region`: From hosts table

### Next Steps: Build the Exporter Script
We'll create `src/horizonscale/generate_csv_inputs.py` with a `--resource cpu --month OCT2024` mode.

**Plan for the Script**:
1. Import config.py for paths/tables.
2. Connect to DB.
3. Query physical hosts + time_periods for October 2024.
4. Generate cpu_p95 per host/day (simple random normal around 50%).
5. Export to CSV using Polars.

Ready? I'll provide the full script in the next response once you confirm "Ready for CPU exporter script".  

Commit any notes:
```powershell
git add docs/phase_b_2_csv_file_generation_planning.md
git commit -m "Plan baby steps: Start with CPU p95 CSV generation"
git push
```

Let me know when you're set! 🚀