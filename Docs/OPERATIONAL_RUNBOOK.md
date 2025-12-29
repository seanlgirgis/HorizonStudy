# Operational Runbook: HorizonScale Execution Guide

## Overview

This runbook provides the specific command-line instructions required to execute the HorizonScale capacity utilization pipeline. It follows a modular sequence designed to move from data generation to high-performance forecasting and final risk visualization.

---

## 1. System Initialization

Before running the pipeline, ensure the environment is initialized and all core libraries are available.

* **Working Directory:** Navigate to the root of the project source.
* **Global Config:** Verify parameters in `src/HorizonScale/lib/config.py`.

## 2. Phase 1: Synthetic Data Foundation

These commands establish the 3-year historical baseline of utilization data for 2,000 servers.

```bash
# Initialize the local database and folder structure
python3 src/HorizonScale/synthetic/00_init_db.py

# Generate 3 years of historical utilization data (Parquet format)
python3 src/HorizonScale/synthetic/01_generate_master_parquet.py

# Export the master data into monthly CSV feeds for pipeline ingestion
python3 src/HorizonScale/synthetic/02_export_monthly_csvs.py

```

## 3. Phase 2: The Forecasting Pipeline

This phase ingests the monthly feeds and executes the high-speed "Turbo" forecasting engine.

```bash
# Process raw monthly feeds into the unified data pipeline
python3 src/HorizonScale/pipeline/03_data_pipeline.py

# Run baseline forecasting for all server nodes
python3 src/HorizonScale/pipeline/04_baseline_forecasting.py

# Execute Turbo Prophet: High-performance multiprocessing forecasting
# This bypasses the Python GIL to utilize all available CPU cores
python3 src/HorizonScale/pipeline/06_turbo_prophet.py

# Execute Challenger logic to compare baseline vs. new forecasts
python3 src/HorizonScale/pipeline/07_baseline_challenger.py

# Run Model Competition to select the mathematically optimal model
python3 src/HorizonScale/pipeline/08_model_competition.py

```

## 4. Phase 3: Risk Reporting and Dashboards

Finalize the execution by generating actionable reports and launching the interactive visual interface.

```bash
# Generate risk reports based on the 6-month outlook
python3 src/HorizonScale/pipeline/09_risk_reporting.py

# Launch the interactive Risk Dashboard via Streamlit
streamlit run src/HorizonScale/pipeline/10_risk_dashboard.py

```

## 5. Operational Verification

To confirm a successful run, verify the following outputs:

* **Historical Data:** Check for `master_utilization.parquet` in the data directory.
* **Forecast Results:** Verify the existence of the competitive model selection logs.
* **Risk Output:** Ensure the `high_trust_exceptions.csv` has been updated with the latest timestamps.

## 6. Troubleshooting Command

If a process in the "Turbo" phase hangs, you can identify and monitor the multiprocessing pool using:

```bash
# Monitor active Python processes and CPU load in linux
top -u $(whoami)

# Monitor active Python processes and CPU load in Windows using the performance monitor

```