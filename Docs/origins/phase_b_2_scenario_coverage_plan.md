# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 2: CSV File Generation – Ensuring Scenario Coverage for ML Results**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

Congratulations on generating the first data file (`cpu_p95_OCT2024.csv`) – this is a key milestone! To answer your question: Ensuring the data supports diverse scenarios (steady growth, seasonal patterns, sudden bursts, low/idle utilization, capacity breaches, plateaus/declines) is crucial for demonstrating strong ML results (e.g., accurate forecasts, anomaly detection, and actionable alerts). We can't rely on random noise alone; we must intentionally embed these patterns during generation.

### How to Guarantee Scenario Coverage
We'll use a structured approach to "inject" scenarios into the time-series data (e.g., cpu_p95, vcpu_p95) for each host:
- **Assign Scenarios per Host**: Add a new DB column `scenario_type` in `hosts` (e.g., 'steady_growth', 'seasonal', 'burst', etc.) during seeding. Distribute evenly (e.g., 16% per scenario for 6 types).
- **Generate Util Patterns Based on Scenario**: In the exporter script, check the host's scenario and apply math formulas (using NumPy/Polars):
  - **Steady Growth**: Linear increase (e.g., +1–5% per month).
  - **Seasonal Patterns**: Sine waves for cycles (e.g., weekly Mon-Fri peaks; yearly Christmas/New Year spikes in Dec/Jan, Super Bowl in Feb, summer holidays in Jul/Aug).
  - **Sudden Bursts**: Random 2–3 day spikes (90–100% util).
  - **Low/Idle Utilization**: Constant <10% util (for exclusion testing).
  - **Capacity Breaches**: Trend toward exceeding max_cores/memory_gb within 3–6 months.
  - **Plateaus/Declines**: Flat or decreasing util (e.g., -2% per month).
- **Balance Distribution**:  Ensure ~16% of hosts per scenario (configurable in config.py).
- **Validation**: After generation, add a check script to verify balance (e.g., count hosts with breaches).
- **Realism Controls**: Cap at 100%, add noise (normal distribution), correlate with region/department (e.g., Retail spikes during holidays).

This ensures the data is "ML-ready" – models like Prophet will detect seasonality, TFT hybrids will handle bursts, leading to <8% MAPE and clear dashboard visuals.

### Implementation Plan (Baby Steps)
1. Update `hosts` schema with `scenario_type` column.
2. Modify `generate_synthetic_data.py` to assign scenarios during seeding.
3. Update `generate_csv_inputs.py` to use scenario for util generation.

### Suggested Doc Note
**File Name**: `docs/phase_b_2_scenario_coverage_plan.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_2_scenario_coverage_plan.md
git commit -m "Plan for scenario coverage in synthetic data generation"
git push
```

Ready for the schema update and script modifications? Let me know – we'll do it in the next response. 🚀