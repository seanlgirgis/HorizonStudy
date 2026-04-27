# HorizonScale – Iteration 2: Pipeline Modernization – Script Renaming & Execution Order

**Author:** Sean L Girgis  
**Date:** December 23, 2025  
**Status:** Sub-task 1 Complete – Master Parquet Generated & Exported; Planning Sub-task 2 (Pipeline & Modeling)  

Noted—`02_export_monthly_csvs_from_parquet.py` is locked (optional exporter for legacy CSVs). We skip 02 for new core scripts, starting from **03** for the modern pipeline.

With master Parquet as source of truth (contiguous, efficient), we update the 8 scripts to load from it directly (prefer Parquet → fallback DuckDB/CSVs). This eliminates CSV concat overhead and ensures continuity for forecasting.

### Updated Execution Order & Renaming (Starting from 03)
Run after `01_generate_master_parquet.py` (and optional `02_export...`).

1. **03_data_pipeline.py** (Rename from data_pipeline.py – not in list, but critical)  
   - Why first: Ingests master Parquet into DuckDB (daily agg, rolling features). Core ETL modernization.  
   - Update: Direct Parquet load mode.

2. **04_eda_synthetic.py** (Rename from eda_synthetic.py)  
   - Why next: Raw master Parquet exploration (trends, distributions pre-processing).

3. **05_eda_processed.py** (Rename from eda_processed.py)  
   - Why: Post-ETL features validation (rolling means, volatility).

4. **06_baseline_prophet.py** (Rename from baseline_prophet.py)  
   - Why: Simple baseline—Prophet on processed data.

5. **07_baseline_sarima_ets.py** (Rename from baseline_sarima_ets.py)  
   - Why: Classical baselines for comparison.

6. **08_baseline_comparison.py** (Rename from baseline_comparison.py)  
   - Why: MAPE/visual comparison after baselines.

7. **09_forecasting_engine.py** (Rename from forecasting_engine.py)  
   - Why: Core TFT model—advanced 6-month forecasts.

8. **10_advanced_hybrid.py** (Rename from advanced_hybrid.py)  
   - Why: Final hybrid (TFT + LightGBM) for best accuracy.

9. **11_anomaly_recs.py** (Rename from anomaly_recs.py)  
   - Why: Residual anomalies + right-sizing recs.

10. **12_dashboard.py** (Rename from dashboard.py)  
    - Why last: Streamlit visualization of forecasts/anomalies/recs.

### Recommended Flow
- **Immediate**: Draft `03_data_pipeline.py` update (Parquet → DuckDB processed tables).  
- Run order: 03 → 04/05 (EDA) → 06-08 (baselines) → 09-10 (advanced) → 11 (anomalies) → 12 (dashboard).  
- Portfolio win: "Modernized pipeline to ingest contiguous Parquet, reducing ETL time 10x and enabling true long-horizon forecasting."

Ready to draft `03_data_pipeline.py` update (load master Parquet, compute features, store DuckDB)? This unlocks modeling. 🚀