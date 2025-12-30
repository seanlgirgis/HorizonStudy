# Operational Runbook: HorizonScale

## 1. System Pre-Requisites

Before initiating the pipeline, ensure the environment is correctly configured:

* **Python Version:** 3.8+ required for `multiprocessing` stability.
* **Hardware:** Minimum 4 CPU cores recommended to utilize the "Turbo" engine.
* **Dependencies:** Install requirements via `pip install -r requirements.txt`.

## 2. Pre-Flight Health Check

To prevent execution errors, verify the integrity of the 3-year historical foundation before starting the forecasting engine.

### **Database Verification Command**

Execute the following to ensure the master Parquet file is present and contains the required 2,000 nodes:

```bash
# Verify the existence and size of the historical master file
ls -lh data/processed/master_utilization.parquet

```

* **Health Indicator:** If the file size is 0 or missing, re-run the synthetic data initialization script (`00_init_db.py`).
* **Integrity Gate:** The pipeline will automatically exit if the 36-month look-back window is incomplete.

## 3. Core Pipeline Execution

The pipeline follows a specific execution sequence to ensure data consistency.

### **Step 1: Data Initialization**

Generates the synthetic 3-year history for 2,000 server nodes.

```bash
python3 src/HorizonScale/synthetic/00_init_db.py

```

### **Step 2: The Turbo Forecasting Engine**

Launches the multiprocessing pool to calculate forecasts.

```bash
python3 src/HorizonScale/pipeline/06_turbo_prophet.py

```

### **Expected Terminal Output (Healthy Execution)**

When successfully initialized, the terminal should show the distribution of tasks across CPU cores:

```text
[INFO] Initializing HorizonScale Turbo Engine...
[INFO] Detected 16 logical CPU cores. Initializing Pool...
[INFO] Distributing 2,000 nodes across 16 processes.
[SUCCESS] Process 0: Completed 125 nodes.
[SUCCESS] Process 1: Completed 125 nodes...

```

## 4. Reporting & Visualization

Once the "Turbo" engine completes, generate the risk audit summaries.

### **Step 3: Risk Audit Generation**

```bash
python3 src/HorizonScale/pipeline/09_risk_reporting.py

```

### **Step 4: Launching the Dashboard**

```bash
streamlit run src/HorizonScale/pipeline/10_risk_dashboard.py

```

## 5. Troubleshooting

* **Memory Errors:** If the system runs out of RAM, reduce the `chunk_size` parameter in `config.py`.
* **GIL Bottlenecks:** Ensure you are using `06_turbo_prophet.py` (multiprocessing) rather than the legacy sequential script to ensure core utilization.

