docs/pipeline/07_baseline_challenger.md

### **File Overview: `07_baseline_challenger.py**`

The `07_baseline_challenger.py` script functions as the **Challenger Engine** for the HorizonScale project. It orchestrates the training and inference of a Gradient Boosted (XGBoost) regressor to serve as a high-performance alternative to the baseline models. This script is engineered to handle structural complexity and non-linear patterns within the telemetry of approximately 8,000 asset pairs.

---

### **1. GPU-Accelerated Architecture**

The script utilizes advanced machine learning techniques to maximize processing speed and efficiency:

* **CUDA Offloading**: Leverages CUDA-enabled 'hist' tree algorithms to offload intensive training tasks directly to the GPU.
* **Feature Engineering**: Maps temporal data into ordinal trend components (`t`) and seasonal cycle components (`month`) to enable the model to understand complex time-series patterns.
* **High-Throughput Hyperparameters**: Employs specialized settings for GPU stability, allowing for rapid iteration across the entire server fleet.

### **2. Competitive Modeling Logic**

To participate in the project's "Tournament Layer," the challenger follows a standardized validation framework:

* **Standardized Training Split**: Operates on a consistent 32-month training window to ensure fair benchmarking against other models.
* **Backtest and Forecast Horizon**: Constructs a future timeline encompassing a 4-month competitive backtest and a 6-month forward-looking forecast.
* **Interval Estimation**: Implements manual calculation for lower and upper bounds to provide prediction intervals comparable to other modeling outputs.

### **3. Modeling Engine Workflow**

Each asset undergoes an isolated modeling lifecycle managed by the core orchestration logic:

* **Data Partitioning**: Uses high-speed database extraction to group global telemetry by unique asset identifiers.
* **Inference and Prediction**: Generates point predictions based on the learned trend and seasonal components.
* **Metadata Tagging**: Categorizes all output as either "backtest" or "forecast" and tags records with the specific host ID, resource, and model type for downstream analysis.

### **4. Persistence and Verification**

The script ensures the integrity of the generated projections through a robust persistence layer:

* **Atomic Flushes**: Consolidates all individual modeling results into a single master Parquet file to ensure data consistency.
* **Relational Integration**: Flushes results into the primary DuckDB store, creating the `challenger_results` table for standardized model competition.
* **Performance Auditing**: Utilizes an execution timer to monitor for bottlenecks and provide a clear report on the total GPU processing duration.

---

### **System Requirements**

* **Entrance Criteria**: Refined telemetry in the database and a valid configuration for data directories.
* **Exit Criteria**: Successful population of the `challenger_results` table and verified Parquet persistence files.
* **Dependencies**: Requires `xgboost` with CUDA support, `duckdb` and `polars` for data management, and `pandas` for feature engineering.