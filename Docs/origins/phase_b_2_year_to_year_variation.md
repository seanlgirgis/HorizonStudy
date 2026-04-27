# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 2: CSV File Generation – Mechanism for Year-to-Year Variation**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

When generating data for 2023 first and then 2024 (or any sequence), the script does **not** maintain "current state" from previous runs. The variation mechanism is **stateless** – each year (or month) is generated independently using deterministic formulas based on `scenario_type`, with random elements (noise, bursts) seeded for reproducibility.

### How Year-to-Year Variation Works
- **Independent Generation**:  
  - For each run (e.g., `--year 2023`), the script queries the full time_periods (2023–2025) but computes util_series for the entire period from the same starting base (e.g., 40.0 for 'steady_growth').  
  - It then filters only the requested year's rows for CSV export.  
  - When you run `--year 2024`, it re-computes the **same util_series** from the same base and formulas – so the values for 2024 are a continuation of the 2023 trend (e.g., linspace continues from where it left off in 2023).

- **LineSpace Continuity**:  
  - Linspace is over the full 30 months (e.g., 0 to 30 over 1095 days).  
  - For 2023 (days 1–365): util = base + linspace[0:365]  
  - For 2024 (days 366–730): util = base + linspace[365:730]  
  - This creates smooth continuation – e.g., if a host ends 2023 at 50%, it starts 2024 at 51% in a growth scenario.

- **Random Elements (Noise/Bursts)**:  
  - Use fixed seed (`np.random.seed(42)`) at the start of the function.  
  - Same seed → same random noise/bursts every run.  
  - Result: Re-running 2023 then 2024 gives consistent, continuous series.

### Updated Script Snippet (Add to generate_util_series)
```python
np.random.seed(42)  # Fixed seed for reproducibility
# ... rest of function
```

### Why This Works Well
- **No DB Updates Needed**: No state saved – everything computed on-the-fly.  
- **Smooth Progression**: Growth trends continue seamlessly year-to-year.  
- **Reproducibility**: Same seed ensures identical data every time.  
- **Testing**: Generate 2023, then 2024 – check CSV continuity (last day of 2023 matches first of 2024 in trend).

### Suggested Doc Note
**File Name**: `docs/phase_b_2_year_to_year_variation.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_2_year_to_year_variation.md
git commit -m "Document year-to-year variation mechanism in data generation"
git push
```

Ready to add the fixed seed and generate 2023 + 2024 CSVs? Let me know. 🚀