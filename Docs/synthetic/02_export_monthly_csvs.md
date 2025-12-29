docs/synthetic/02_export_monthly_csvs.md

### **File Overview: `02_export_monthly_csvs.py**`

The `02_export_monthly_csvs.py` script functions as a decomposition engine that simulates a fragmented data environment. It takes the centralized Master Parquet file and breaks it down into individual monthly CSV files. This process replicates the state of raw data feeds before they are ingested into a modern pipeline, allowing the system to demonstrate resilience during the reconstruction process.

---

### **1. Entrance Audit and Genesis Criteria**

The script ensures all prerequisites are met before beginning the data fragmentation process.

* **Source Verification**: Confirms the existence of the Master Parquet file generated in the previous stage.
* **Target Initialization**: Automatically creates the root directory for legacy inputs if it does not already exist.
* **Configuration Mapping**: Loads resource-specific prefixes and capacity field labels from the central configuration to ensure the output matches expected external standards.

### **2. Decomposition Engine**

The core logic maps centralized data back into a partitioned, resource-specific structure.

* **Temporal Partitioning**: Injects calendar columns into the dataset to facilitate grouping by year and month.
* **Hierarchical Export**: Fragments the data into a directory structure organized by `/YYYY/MM/`.
* **Legacy Schema Transformation**: Renames generic telemetry columns to resource-specific aliases (e.g., transforming `p95_util` to `cpu_p95`) and maps capacity values to their corresponding labels.
* **Resource Isolation**: Separates the telemetry into distinct files for each resource type (CPU, Memory, Disk, Network) for every month in the three-year window.

### **3. Quality Audit and Exit Criteria**

Upon completion, the script performs a comprehensive validation to ensure no data was lost during the transition from Parquet to CSV.

* **Row Parity Check**: Aggregates the row counts from every generated CSV file and compares the total against the height of the original Master Parquet file.
* **Referential Integrity**: Asserts that every host, resource, and day record from the source is accounted for in the fragmented output.
* **File Naming Standards**: Verifies that files follow the established naming convention (e.g., `cpu_utilization_202401.csv`).

### **4. Technical Performance**

* **Efficient Aggregation**: Utilizes high-performance dataframe operations to read and count records across the fragmented library.
* **Progress Visualization**: Employs a nested loop with progress tracking to manage the fragmentation of millions of records across multiple years and resources.
* **Storage Management**: Uses standard CSV formatting for the output to simulate the low-performance, "dirty" data state typical of legacy feeds.

---

### **System Requirements**

* **Entrance Criteria**: A valid Master Parquet file and defined resource prefixes in the configuration.
* **Exit Criteria**: A hierarchical directory of CSV files with 100% row parity relative to the source data.
* **Dependencies**: Relies on `polars` for fast data manipulation, `tqdm` for progress monitoring, and the project's central configuration for naming conventions.