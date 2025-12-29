docs/pipeline/06_turbo_prophet.md

### **File Overview: `06_turbo_prophet.py**`

The `06_turbo_prophet.py` script serves as the **Turbo Engine** for enterprise-scale forecasting. It is designed to execute a massively parallelized forecasting fleet, generating projections for over 2,000 hosts—totaling approximately 8,000 individual time-series. This script is specifically optimized to leverage high-core count workstations to bypass traditional sequential processing bottlenecks.

---

### **1. High-Performance Architecture**

The script employs several advanced technical strategies to achieve high throughput:

* **Massive Parallelization**: Utilizes a multiprocessing approach (via `ProcessPoolExecutor`) to bypass the Python Global Interpreter Lock (GIL), allowing CPU-bound forecasting tasks to saturate multiple physical cores simultaneously.
* **Zero-Latency Distribution**: Leverages high-speed memory management to group millions of telemetry rows before distributing tasks to worker processes.
* **Throughput Optimization**: Reduces uncertainty sampling settings in the modeling engine to achieve a 10x throughput gain compared to standard configurations.

### **2. Tournament and Backtest Logic**

To ensure the reliability of the 6-month projections, the script implements a competitive validation split:

* **Training Window**: Models are trained on an extensive historical window (approximately 32 months).
* **Backtest Layer**: A 4-month "backtest" window is established to evaluate model performance against known data before projecting into the future.
* **Future Horizon**: Generates the final 6-month forecast beyond the historical baseline.

### **3. Independent Worker Lifecycle**

Each individual worker process executes an encapsulated modeling lifecycle for a single resource series:

* **Logistic Growth Fitting**: Applies physical constraints to the models, ensuring utilization projections remain within the realistic [0, 100] envelope.
* **Metadata Tagging**: Each projection is tagged with identifying metadata (Host ID, Resource, Model Type) and categorized as either "backtest" or "forecast" for downstream analysis.
* **Error Isolation**: Individual process failures are caught and logged independently to prevent the entire fleet execution from terminating.

### **4. Consolidation and Persistence**

The orchestration logic manages the transition from raw compute to verified data persistence:

* **Dual-Layer Persistence**: Results are persisted to a high-performance Parquet file for rapid dashboard consumption and a relational table for tournament-style model comparisons.
* **Idempotency**: The script automatically clears previous run outputs to ensure a clean state for every execution.
* **Execution Auditing**: Uses a specialized timer to profile and report the total duration of the fleet processing for performance auditing.

---

### **System Requirements**

* **Entrance Criteria**: A populated analytical base table and initialized configuration parameters.
* **Exit Criteria**: Successful population of the results table and generation of a master Parquet file with 100% of telemetry series processed.
* **Dependencies**: Requires `prophet` for modeling, `duckdb` and `polars` for data orchestration, and `concurrent.futures` for parallel execution.