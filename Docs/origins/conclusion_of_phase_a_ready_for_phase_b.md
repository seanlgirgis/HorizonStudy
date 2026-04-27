# HorizonScale – Personal Portfolio Project  
**Official Project Charter & Execution Plan**  
Goal: Build a 100% original, production-grade, resume-defining capacity planning & forecasting system from scratch, inspired by (but never copying) an existing internal tool you have access to.

**Project Owner:** Sean L Girgis  

**Target Completion:** 3–4 weeks (fast but extremely high quality)  
**Outcome:** Public GitHub repository + 12–18 page technical paper + killer resume bullets + interview-ready talking points

### Phase A Discovery Report: Analysis of Original Trenda System

**Prepared by:** Sean L Girgis (Author)  
**Date:** December 20, 2025  
**Project:** HorizonScale  
**Phase:** A – Discovery & Knowledge Extraction  
**Confidential:** For internal use only – private project notes.

This report summarizes the analysis of provided materials: the textual Word document ("textual Doc.docx") describing the Trenda application overview, design approach, jobs/modules, and user guide; along with code excerpts from key Python scripts and modules. All content has been reviewed and annotated to extract core system elements. The focus remains on understanding the original system without copying any code.

Trenda is a mature, production-grade infrastructure capacity forecasting tool, likely used internally at a large enterprise (e.g., references to "cticman1" queues and Cloudera parcels). It processes time-series data for resources like CPU, vCPU, memory, and storage, producing forecasts, exceptions, and notifications. The system leverages Big Data technologies (Spark, Hive, Python) with a modular architecture for scalability.

#### Table of Contents
- [1. Data Sources & Schema](#1-data-sources--schema)
- [2. Preprocessing Steps](#2-preprocessing-steps)
- [3. Features Used](#3-features-used)
- [4. Models and Algorithms](#4-models-and-algorithms)
- [5. Evaluation Method & Metrics](#5-evaluation-method--metrics)
- [6. Forecast Generation & 3-Month Decision Horizon](#6-forecast-generation--3-month-decision-horizon)
- [7. Business Rules & Alerting Logic](#7-business-rules--alerting-logic)
- [Mind-Map of the Original Architecture](#mind-map-of-the-original-architecture)
- [Planned Improvements & Modernizations](#planned-improvements--modernizations)

#### 1. Data Sources & Schema
Data is ingested from monthly CSV feeds and stored in Hive tables partitioned by year-month (e.g., "YYYY-MM").

- **Resource Metrics (P95 Files)**: Monthly CSVs for CPU, vCPU, Memory (MEM), and Storage (STO) with percentile-based utilization (e.g., P95). File names encode dates (e.g., "MMMYYYY" like "OCT2019"). Key columns include: `node_name` (or `node_resource` for storage), `date`, `cpu_p95` (or `p95resmem_util`, `P95_Storage_Util_Pct`), `yearmonth` (partition key), `region`, `max_engines` (capacity). Storage adds `Storage_Capacity_mb`.
  - Regions: ASIAPAC, EMEA, LATAM, NA – processed separately.
  - Annotation: Raw daily/weekly time-series, aggregated monthly for forecasting; exclusions track ineligible nodes.

- **Configuration Files (CON)**: Paired CSVs for server and ESX Guest config (updated format post-March 2024 with `CI_NAME`, `GOC`, `LE_ID`).
  - Includes business hierarchies (e.g., `app_manager_goc`, `business_owner_le_id`).

- **Support/CMDB Data**: CSI files for app ownership, roles, emails, and functionality (e.g., `csi_basic_functionality` table).
  - Annotation: Schemas evolve with hierarchy columns for KPI support.

#### 2. Preprocessing Steps
Handled in mod_exclude.py:

- Ingestion & Partitioning: Load CSVs, validate cycle dates, append to Hive tables.
- Filtering & Exclusion: Apply rules for forecast eligibility (e.g., min active days ~15+, util >5% threshold). Log exclusions (e.g., low util, insufficient data).
- Sanitization: Outlier capping (e.g., 100%), interpolation, weekly rollup.
- Annotation: Region-specific for scale; business rules filter early to reduce compute.

#### 3. Features Used
Time-series focused:

- Core: `date` (ds), `p95_util` (y/target), `capacity` (e.g., max_engines).
- Derived: Weekly/monthly aggregates, uncertainty bands, risk-break dates.
- Enrichments: CMDB joins for business context (e.g., app_manager, hierarchies).
- Exogenous: Holidays integrated.
- Grouping: By `node_name`, `region`, `node_resource`.
- Annotation: Minimal engineering; relies on raw P95 metrics.

#### 4. Models and Algorithms
- Primary: Facebook Prophet (time-series library).
  - Seasonality: Yearly/weekly; holidays via holidays==0.17.
  - Changepoints: Auto/custom.
  - Uncertainty: MCMC sampling for intervals.
  - Parallel: Pandas UDF on Spark for node-batch forecasting.
- Legacy: Anticipy toolbox (SARIMA/ETS).
- Annotation: Prophet handles trends/seasonality/anomalies natively.

#### 5. Evaluation Method & Metrics
- Metrics: RMSE for model selection; MAPE targeted <8%.
- Method: Compare forecast vs. historical holdout.
- Post-Processing: Visual charts; flag breaches.
- Annotation: Basic holdout; could add advanced CV.

#### 6. Forecast Generation & 3-Month Decision Horizon
- Input: 6-12 months history.
- Process: Train Prophet, forecast 6 months (180 periods).
- Output: `yhat`, bounds; regional, then combined.
- 3-Month Focus: Prioritize near-term risks (e.g., flag breaks within 90 days).
- Annotation: Aligns with actionable insights.

#### 7. Business Rules & Alerting Logic
- Rules: Exclusions (low util/inactive), risk-breaks if forecast > capacity.
- Alerting: Notify/escalate tables; SMTP emails to managers (suppressible).
- Other: MASTER_SWITCH (read-only); cycle partitioning.
- Annotation: Rule-based; no advanced ML anomaly detection.

#### Mind-Map of the Original Architecture
(Textual; render as Mermaid for README.)

```
Trenda System
├── Data Ingestion (mod_loader.py)
│   ├── Sources: P95 CSVs, CON CSVs, CMDB/CSI
│   └── Output: Partitioned Hive Tables
├── Preprocessing (mod_exclude.py)
│   ├── Filters & Exclusion
│   └── Output: Eligible data + Exclusions
├── Forecasting (mod_forecast_pr_new.py)
│   ├── Model: Prophet (Pandas UDF)
│   └── Output: Regional Forecasts
├── Aggregation (mod_combine.py)
│   └── Output: Consolidated Charts
├── Output Generation (mod_generate.py)
│   ├── Enrich & Alerts
│   └── Output: GUI Tables, CSVs, Emails
├── Helpers
│   ├── mod_config.py
│   ├── mod_common.py
│   └── mod_resdb.py
└── Runtime: Spark/YARN, Hive, Python 3.9
```

#### Planned Improvements & Modernizations
1. **Data Pipeline**: Polars + DuckDB; synthetic data for 1,000–5,000 hosts.
2. **Preprocessing**: Advanced outliers (Isolation Forest), spline interpolation, YAML rules.
3. **Models**: Baseline SARIMA/ETS; advanced NeuralProphet/Darts/TFT + LightGBM.
4. **Forecasting**: Explicit 6-month with 3-month highlights; built-in anomaly detection.
5. **Evaluation**: Time-series CV; added metrics; hyperopt selection.
6. **Output/UI**: Streamlit dashboard with GIFs; right-sizing recommendations.
7. **Infra**: Tests, CI, Docker.
8. **Scalability**: Cloud optimization.
9. **Modern Touches**: MLflow tracking, SHAP explainability, API endpoints.
10. **Documentation**: Sphinx API docs.

This concludes Phase A. Ready for Phase B.