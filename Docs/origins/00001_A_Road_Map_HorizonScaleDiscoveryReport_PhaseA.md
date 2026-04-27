# HorizonScale Discovery Report (Phase A)

**Author:** Sean L Girgis  
**Date:** December 25, 2025  
**Version:** 1.0 (Private – For Internal Project Use Only)  
**Purpose:** Summarize key elements of the original Trenda system based on provided documentation and code screenshots (textual Doc.docx and implied image1.png). This report extracts critical insights to inspire HorizonScale's design without copying any code or proprietary logic. Focus on architecture, data flow, models, and opportunities for modernization.

## Executive Summary (1/2 page equivalent)
The original Trenda system is a capacity forecasting tool for infrastructure resources (CPU, VMCPU, MEM, STO) built on Hadoop, PySpark, Hive, and Python. It processes monthly data feeds, applies exclusion filters, forecasts using a toolbox (likely including Prophet), combines regional forecasts, and generates reports/emails with risk alerts. The core is a modular stub-module design with 5 processes: loader, exclude, forecast, combine, generate. Forecasts span 6 months, with emphasis on a 3-month "decision horizon" for actions like right-sizing or alerts.

Key strengths: Integration with big data ecosystems, rule-based filtering for forecast viability, and automated email notifications. Limitations: Reliance on legacy tools (Hive, PySpark), potential single-model dependency (Prophet), manual monthly runs, and lack of advanced ML (e.g., no neural networks or hybrids). No mention of anomaly detection, uncertainty bands, or interactive dashboards.

HorizonScale will modernize this by using Polars/DuckDB for faster pipelines, synthetic data for testing, hybrid models (TFT + LightGBM) for <8% MAPE, built-in anomaly detection, and a Streamlit dashboard. Improvements target scalability, accuracy, and usability.

## Data Sources & Schema (1/2 page)
- **Sources:** Monthly data feeds ingested via loaders (e.g., CPU/MEM/STO from various systems). Data is partitioned by month in Hive tables. Specific loaders handle resource types: e.g., CPU_P95, MEM_P95, STO_P95, VMCPU_P95. Feeds include historical utilization (P95 metrics) and capacity details (e.g., max_cores for CPU).
- **Schema:** Resource tables in Hive with monthly partitions. Key fields inferred from processes:
  - Date/timestamp (monthly observations).
  - Node/host ID (e.g., for filtering viable series).
  - Utilization metrics (e.g., CPU_P95, MEM utilization).
  - Capacity (e.g., max_cores, storage capacity).
  - Region (for regional forecasts).
  - Additional metadata: Low utilization thresholds, observation counts, outliers.
- **Volume/Scale:** Handles thousands of nodes (implied by big data stack). Data sanitization includes outlier detection and interpolation.
- **Ingestion:** mod_loader.py creates partitions and ingests feeds. Commands like "load cpu" process specific resources.

No explicit schema diagram in docs, but processes imply a star-like structure: raw resource tables → filtered/excluded → forecasted → combined.

## Preprocessing Steps (1/2 page)
- **Loader:** Ingests raw feeds, partitions by month.
- **Exclude:** Core preprocessing module (mod_exclude.py).
  - Viability filters: Thresholds for monthly observations (e.g., min count to forecast).
  - Constraint filters: Excludes low-utilization nodes (e.g., historical util < threshold).
  - Data sanitization: Outlier analysis (likely statistical methods like IQR or Z-score), interpolation for missing values.
  - Output: Qualified data series ready for forecasting (makes "life easier" for models).
- **General:** Command-line parsing for modes (e.g., "exclude cpu"). Logging for stats/runtime. Integration with Spark/Hive for distributed processing.
- **Business Rules:** Implicit rules like low-util thresholds to avoid forecasting idle resources.

No mention of feature engineering (e.g., no rolling windows or seasonality extraction), but outlier handling suggests basic stats.

## Features Used (1/4 page)
- Primary: Historical P95 utilization time series per node/resource/region.
- Filters: Observation count, historical util levels, outliers.
- No advanced features noted (e.g., no lags, trends, or external covariates like holidays/business events).
- Implied: Date-based for seasonality, node metadata for grouping.

## Models and Algorithms (1/2 page)
- **Forecast Module:** mod_forecast.py uses a "toolbox of forecast models" to select the best per series. Docs mention "Prophet-related forecast jobs" with 'pr_new' qualifier, strongly implying Facebook Prophet as primary (time-series with trend/seasonality). Possible others: SARIMA or custom stats models (not specified).
- **Resources Forecasted:** CPU, VMCPU, MEM, STO – separately by region.
- **Selection:** Chooses "best model" (criteria not detailed – likely cross-validation or AIC).
- **Output:** Regional forecasts, later combined.
- No mention of ML hybrids, neural nets, or ensemble methods. Uncertainty not explicit (no bands noted).

## Evaluation Method & Metrics (1/4 page)
- Not explicitly detailed in docs. Implied via "best model" selection – likely in-sample metrics like MAPE, RMSE, or AIC for stats models.
- No out-of-sample validation or backtesting mentioned.
- Shutdown stats/logs track runtime/errors, but not forecast accuracy.
- Business focus: Risk-break stats in CSV for review (e.g., exceptions, alerts).

## How They Generate the 6-Month Forecast and Focus on 3-Month (1/2 page)
- **Forecast Generation:** Post-exclude, mod_forecast produces 6-month ahead forecasts per resource/region using selected model (e.g., Prophet).
- **Combination:** mod_combine aggregates regional forecasts by resource, then "combine all" consolidates across resources.
- **Decision Horizon:** Emphasis on first 3 months for actionable insights (e.g., right-sizing, capacity breaches). Generate process produces CSVs/emails highlighting risks in this window.
- **Business Rules/Alerting:** 
  - SPINOFF_SWITCH/MASTER_SWITCH control email modes (trial vs. prod).
  - Risk-break stats: Exceptions, high-risk nodes (e.g., util > thresholds).
  - Emails to app managers with stats (e.g., "Prod Cycle Complete").
  - Manual review: CSV in /data/app/cticman1/detailcsv for pre-email checks.
  - Portal push: Detailed CSV to GER/GFER portals.
  - Backups: TSCO-facing tables post-cycle.

Full cycle: Monthly run (steps 1-14) with waits for dependencies.

## Mind-Map of the Original Architecture (Text-Based Representation)
```
Trenda System (Hadoop/PySpark/Hive/Python)
├── Stub Jobs (149634_tr_'NAME'.py)
│   ├── Setup: Imports, Spark/Hive init, CLI parse, Logger, Config
│   └── Call Module Driver
├── Modules (mod_'NAME'.py)
│   ├── Loader: Ingest feeds → Monthly Hive partitions (per resource)
│   ├── Exclude: Viability filters → Sanitize (outliers/interpolate) → Qualified series
│   ├── Forecast: Toolbox models (Prophet?) → 6-mo forecasts per resource/region
│   ├── Combine: Aggregate regions → Consolidate resources
│   └── Generate: CSVs/emails with risk stats (3-mo focus) → Portal push → Backups
├── Config (mod_config.py): Switches (SPINOFF/MASTER) for email control
├── Logging: Shutdown stats/runtime/errors
├── Monthly Cycle: 14 steps (load/exclude/forecast/combine/generate per resource + all)
└── Outputs: CSVs (detail/risk), Emails (sample/prod), Portal uploads
```

## List of Improvements/Modernizations for HorizonScale
1. **Data Pipeline:** Replace Hive/PySpark with Polars/DuckDB for 10x faster, in-memory processing. Add synthetic generator for scalable testing (1k-5k hosts with scenarios like bursts/breaches).
2. **Preprocessing:** Enhance with rolling features (7-day mean/std), advanced anomaly detection (Isolation Forest), and auto-interpolation.
3. **Models:** Baseline SARIMA/ETS + advanced TFT/LightGBM hybrid for <8% MAPE (vs. implied Prophet-only). Add uncertainty bands (95% CI).
4. **Forecast Horizon:** Retain 6-mo with 3-mo decision window, but add risk levels (critical/high/medium) and breach probability.
5. **Anomaly/Recommendations:** New: Auto-detect anomalies on residuals, generate right-sizing recs (e.g., "URGENT scale-up").
6. **Automation/UI:** Streamlit dashboard for interactive viz (vs. CSVs/emails). GitHub Actions CI, unit tests, Docker for deployment.
7. **Scalability:** Handle diverse scenarios (seasonal/burst/idle/decline), correlations (rack/cluster), and real-time potential.
8. **Evaluation:** Explicit backtesting with MAPE/RMSE, model comparison plots/tables.
9. **Security/Compliance:** Synthetic data avoids real feeds; no big data dependencies.
10. **Overall:** Cleaner code (modular, typed), faster runs (<7 days to forecasts), employer-ready docs (paper/deck/resume).

This report confirms full understanding of Trenda. Phase A signed off – ready for Phase B coding. Let's build HorizonScale! 🚀