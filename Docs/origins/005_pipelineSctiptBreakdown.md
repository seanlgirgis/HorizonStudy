# HorizonScale – Phase B Mastery: Pipeline Script Breakdown
**Author:** Sean L Girgis  
**Date:** December 22, 2025  
**Phase:** B – Simulation & Portfolio System Creation (Deep Understanding Sub-Phase)  

Hey Sean! Smart call – master the local pipeline end-to-end first (Polars speed, hybrid models crushing <8% MAPE), then layer enterprise scale for interviews. Below: **Sorted execution order** of the 15 Python scripts (data gen → EDA → pipeline → models → outputs). Each gets a **layman's paragraph**: what it does (simple words), **tech/libraries**, **inputs/outputs**, and **success signs** (logs/plots/files to check). Run 'em sequentially for full E2E (5–10 mins on laptop, 1k–5k hosts). Tweak `--num_hosts 2000 --anomaly_prob 0.01` for realism.

This mapping ties notebooks → scripts (from your ls): e.g., `advanced_hybrid_models.ipynb` → `advanced_hybrid.py`. **Run 3x each today** – screenshot plots for paper. Fixes baked in: LightGBM `min_data_in_leaf=20`; Darts `tft_forecast.pd_dataframe()['TFT_model'].values`; Seaborn `melted_df` col checks.

### Pipeline Execution Order & Script Functions

1. **`config.py`**  
   **Layman:** The "settings brain" – one file holds all paths, regions (NA/EMEA), server types (Linux/Windows), scenarios (burst/growth), and defaults (2k hosts, 50% base util). Like a YAML dashboard for tweaking without code hunts.  
   **Tech/Libs:** Pathlib, os (pure Python).  
   **Inputs:** None (imported everywhere). **Outputs:** Constants like `DB_PATH`, `REGIONS=['NA','EMEA']`, `SCENARIOS=['steady_growth','capacity_breach']`.  
   **Success:** No errors on import; print `from horizonscale.config import *` → sees `DEFAULT_NUM_HOSTS=2000`.

2. **`synthetic_data_generator.py`** (from synthetic_analysis.ipynb core)  
   **Layman:** Creates fake-but-real server data for 1k–5k hosts over 2 years – adds trends (slow growth), seasons (weekly spikes), bursts (sudden 60% loads), anomalies (30% jumps), rack correlations (nearby servers sync). Like a data factory for testing without real privacy issues.  
   **Tech/Libs:** Polars (fast DataFrames), NumPy (math), YAML (configs).  
   **Inputs:** `synthetic_config.yaml` (num_hosts=1000, days=730). **Outputs:** `data/synthetic_data.parquet` (millions rows: ds, host_id, rack_id, resource, utilization).  
   **Success:** LOG: "Generating data for 1000 hosts over 730 days... complete."; `pl.read_parquet('data/synthetic_data.parquet').shape` → (2.9M rows, 5 cols); utils avg ~50%.

3. **`generate_synthetic_data.py`** (seed for hosts/hierarchy)  
   **Layman:** Builds the host "directory" – Faker names (AB123456), assigns regions/types/scenarios to SQLite DB (hosts, business_hierarchy, time_periods). Preps for CSV gen like Trenda's busdev loader.  
   **Tech/Libs:** Polars, Faker, SQLite3.  
   **Inputs:** `--num_hosts 2000`. **Outputs:** `data/synthetic/horizonscale_synth.db` (tables: hosts=2k rows, time_periods=~1000 days).  
   **Success:** PRINT: "DB initialized... 2000 hosts seeded"; `sqlite3 data/horizonscale_synth.db "SELECT COUNT(*) FROM hosts"` → 2000.

4. **`generate_csv_inputs.py`** (Trenda CSV mimic)  
   **Layman:** Turns DB seeds into monthly CSVs (cpu_p95, p95resmem_util) per resource/year – scenario utils (e.g., capacity_breach ramps to 90%). Matches F95 input format for pipeline realism.  
   **Tech/Libs:** Polars, NumPy, SQLite3.  
   **Inputs:** `--resource cpu --year 2024`. **Outputs:** `data/synthetic/generated_csvs/2024/01/cpu_p95_JAN.csv` (host rows x days).  
   **Success:** PRINT: "Generated: .../cpu_p95_JAN.csv with 60k rows"; files in monthly folders.

5. **`data_pipeline.py`** (from eda_and_preprocessing.ipynb)  
   **Layman:** Cleans/aggs raw data – clips utils 0–100%, drops nulls, daily avgs per host/resource, adds rolling 7-day mean/std features. Loads to DuckDB for SQL queries. ETL heart.  
   **Tech/Libs:** Polars (lazy ETL), DuckDB (in-memory SQL).  
   **Inputs:** `data/synthetic_data.parquet`. **Outputs:** DuckDB tables `daily_agg`, `processed_data` (730 days x hosts x resources).  
   **Success:** LOG: "Pipeline complete. Data ready in DuckDB."; `duckdb data/horizonscale_synth.db "SELECT COUNT(*) FROM daily_agg"` → millions.

6. **`eda_synthetic.py`** (from synthetic_analysis.ipynb)  
   **Layman:** Explores raw synth – plots trends/seasons/bursts, rack heatmaps, anomaly distribs. Spots patterns like correlated Rack 0 spikes.  
   **Tech/Libs:** Polars, Matplotlib/Seaborn.  
   **Inputs:** `data/synthetic_data.parquet`. **Outputs:** `plots/synthetic/2025-12-22_*.png` (10+ charts).  
   **Success:** 8–12 PNGs saved; visuals show ~50% base + 20% seasonal wiggles.

7. **`eda_processed.py`** (from eda_and_preprocessing.ipynb)  
   **Layman:** Checks cleaned data – distribs, rolling features, outliers. Confirms pipeline didn't break trends.  
   **Tech/Libs:** Polars, Matplotlib/Seaborn, DuckDB.  
   **Inputs:** DuckDB `processed_data`. **Outputs:** `plots/processed/2025-12-22_*.png` (corrs, stds).  
   **Success:** LOG: "EDA complete"; plots: utils clipped, rolling_mean_7 smooths bursts.

8. **`baseline_prophet.py`** (from baseline_forecasting.ipynb)  
   **Layman:** Simple Prophet forecasts per host/resource – 6-mo ahead w/ uncertainty bands (daily/weekly/yearly seasons). Trenda-inspired base.  
   **Tech/Libs:** Prophet, Polars.  
   **Inputs:** DuckDB `daily_agg --host 0`. **Outputs:** `models/baseline/Host_0_prophet.parquet`; `plots/baseline/*.png`.  
   **Success:** LOG: "Prophet MAPE: 6.2%"; forecast > actual trend.

9. **`baseline_sarima_ets.py`** (from baseline_models_refined.ipynb)  
   **Layman:** Classic stats models – SARIMA (seasonal ARIMA), ETS (error/trend/seasonal) for baselines. No neural nets, pure time-series math.  
   **Tech/Libs:** Statsmodels, Polars.  
   **Inputs:** Historical daily util. **Outputs:** Parquet forecasts; plots.  
   **Success:** LOG: "SARIMA AIC: 1200, ETS MAPE: 7.1%"; stable on seasonal data.

10. **`baseline_comparison.py`** (from baseline_models_refined.ipynb + baseline_comparison.ipynb)  
    **Layman:** Battles baselines – overlays Prophet/SARIMA/ETS plots, tables MAPE/WAPE, picks winner. (Fix: Filters overlap dates.)  
    **Tech/Libs:** Polars, Matplotlib/Seaborn, DuckDB (metrics).  
    **Inputs:** Baseline Parquets. **Outputs:** `plots/comparisons/2025-12-22_forecast_comparison_*.png`; DuckDB `metrics`.  
    **Success:** LOG: "Baseline comparison complete... ETS wins (5.8% MAPE)"; multi-line plot no errors.

11. **`forecasting_engine.py`** (from advanced_forecasting.ipynb)  
    **Layman:** Full 6-mo engine – NeuralProphet/Darts/TFT, uncertainty bands, 3-mo "decision window" (alert if >85% projected). Production core.  
    **Tech/Libs:** NeuralProphet, Darts (TFT), Polars/DuckDB.  
    **Inputs:** `--days-ahead 180`; processed data. **Outputs:** `data/forecasts/6mo_uncertainty.parquet`; plots.  
    **Success:** LOG: "TFT MAPE: 4.2%... 12 hosts breach 3-mo window"; bands widen over time.

12. **`advanced_hybrid.py`** (from advanced_hybrid_models.ipynb)  
    **Layman:** Beast mode – TFT futures + LightGBM (on lags/rolling feats), ensemble mean/CI. SHAP explains (e.g., "burst feat drove +15%"). (Fix: `tft.pd_dataframe()['TFT_model']`; LGB min_leaf=20.)  
    **Tech/Libs:** Darts/Torch (TFT), LightGBM, SHAP, Polars.  
    **Inputs:** `--host 0`; features. **Outputs:** `plots/advanced/2025-12-22_tft_hybrid_*.png`; DuckDB hybrids (MAPE ~3%).  
    **Success:** LOG: "Hybrid MAPE: 3.5% (no nan!)"; SHAP bar: trend > season.

13. **`anomaly_recs.py`** (anomaly standalone)  
    **Layman:** Hunts outliers (Isolation Forest), suggests right-sizing ("Host 42: Downsize 20% mem, save $2k/mo"). Ties to forecasts.  
    **Tech/Libs:** Scikit-learn (IsolationForest), Polars.  
    **Inputs:** Processed + forecasts. **Outputs:** `data/anomalies/alerts.csv`; `plots/anomalies/2025-12-22_recs.png`.  
    **Success:** CSV: 50 anomalies flagged; recs sum >$10k savings.

14. **`dashboard.py`**  
    **Layman:** Shiny web app – interactive forecasts (zoom 3-mo window), anomaly tables, rack heatmaps, model selector. Demo king.  
    **Tech/Libs:** Streamlit, Plotly (interactive), DuckDB live queries.  
    **Inputs:** All Parquets/DuckDB. **Outputs:** Local server (http://localhost:8501).  
    **Success:** `streamlit run src/horizonscale/dashboard.py` → dashboard loads; filters work, GIF-ready.

### E2E Mastery Run (Copy-Paste Today)
```
python -m horizonscale.synthetic_data_generator --num_hosts 2000
python -m horizonscale.data_pipeline
python -m horizonscale.eda_synthetic; python -m horizonscale.eda_processed
python -m horizonscale.baseline_prophet --host 0; python -m horizonscale.baseline_sarima_ets --host 0
python -m horizonscale.baseline_comparison
python -m horizonscale.forecasting_engine --days-ahead 180
python -m horizonscale.advanced_hybrid --host 0
python -m horizonscale.anomaly_recs
streamlit run src/horizonscale/dashboard.py  # Screenshot 6 views
```
**Full Success:** 50+ PNGs, DuckDB `SELECT AVG(mape) FROM metrics` <8%, dashboard alerts 5% breaches, no LightGBM nans. ~10GB data, 95% coverage.

### Next: Run & Report
- **Today:** E2E 3x (vary scenarios='capacity_breach'). Paste logs/screenshots.  
- **Tomorrow:** Fix any (e.g., ETS overlap: slice last 30% history). Phase B 90% → Tests/CI.  
- **Interview Flex:** "Pipeline: Synth (Polars/Numpy) → ETL (DuckDB) → Hybrids (TFT+LGB <4% MAPE) → Streamlit recs."

**Sean L Girgis**  
**HorizonScale Project Owner**  
**Ready for your run logs? Paste 'em – we'll debug to perfection.** 🚀