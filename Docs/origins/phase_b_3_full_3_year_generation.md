# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 1: Synthetic Data Generator – Full 3-Year Generation (2023–2025)**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

Phase B is officially started. With the seed data in place and the first CPU file generated, we now move to generating all resource files for the full historical range (2023–2025) to support robust model training.

### Step 1: Wipe DB and Re-Seed with Smaller Size
To speed development, wipe and re-seed with 2000–3000 hosts:
```powershell
Remove-Item data\synthetic\horizonscale_synth.db
python -m horizonscale.generate_synthetic_data --num_hosts 2500
```

### Step 2: Generate CPU Files for 2023–2025
Run the exporter for each year:
```powershell
python -m horizonscale.generate_csv_inputs --resource cpu --year 2023
python -m horizonscale.generate_csv_inputs --resource cpu --year 2024
python -m horizonscale.generate_csv_inputs --resource cpu --year 2025
```

This creates 36 CSVs (12 months × 3 years) in `data/synthetic/generated_csvs/`.

### Step 3: Next – Generate vCPU, MEM, STO Files
We'll extend the script to support `--resource vcpu/mem/sto`. Update `generate_csv_inputs.py`:
- For vCPU: Filter `classification = 'virtual'`, use `vcpu_p95`.
- For MEM: Use all hosts, `p95resmem_util`.
- For STO: Use all hosts, `P95_Storage_Util_Pct`, `Storage_Capacity_mb`.

Run similar commands for each resource.

### Step 4: Pipeline & Forecasting
Once all CSVs are generated:
- Load into Polars/DuckDB.
- Preprocess (filter, interpolate).
- Train Prophet baseline on CPU data.
- Forecast 6 months ahead (to mid-2026).

Focus on CPU first for end-to-end demo, then replicate.

### Suggested Doc Note
**File Name**: `docs/phase_b_3_full_3_year_generation.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_3_full_3_year_generation.md
git commit -m "Document full 3-year CPU data generation"
git push
```

Run the commands and paste output/success. We'll extend to vCPU next. 🚀