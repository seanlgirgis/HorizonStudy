docs/synthetic/01_generate_master_parquet.md

### **File Overview: `01_generate_master_parquet.py**`

The `01_generate_master_parquet.py` script serves as the high-volume engine of the pipeline. It synthesizes a contiguous 3-year daily telemetry series for the fleet of 2,000+ hosts. By translating behavioral metadata into millions of rows of high-fidelity utilization data, this script establishes the primary dataset used for training models and forecasting.

---

### **1. Verification & Entrance Criteria**

Before beginning intensive computation, the script executes a verification layer to ensure the necessary foundations are in place.

* **Structural Validation**: Checks the DuckDB database for the existence of required inventory and time dimension tables.
* **Fleet Audit**: Confirms the total host count and the number of days in the simulation window to calculate the expected processing volume.
* **Dependency Check**: Verifies that mathematical logic for all scenarios is available in the generation library.

### **2. Telemetry Synthesis Loop**

The orchestration logic manages a high-volume synthesis process that iterates through every host and resource type.

* **Mathematical Synthesis**: Dynamically calls scenario-specific generators based on a host's assigned behavioral DNA (e.g., Steady Growth or Capacity Breach).
* **Deterministic Seeding**: Utilizes a hash-based seeding mechanism for every host/resource combination to ensure the generated telemetry is 100% reproducible.
* **Resource Post-Processing**: Applies specific noise envelopes and adjustment factors for different resource types (CPU, Memory, Disk, Network) to ensure realistic variance across the fleet.
* **Physical Clipping**: Enforces realistic bounds, ensuring all utilization percentages are strictly clipped between 0% and 100%.

### **3. Data Persistence & Performance**

The script is optimized for handling large-scale data structures before persisting them to disk.

* **Vectorized Data Handling**: Uses high-performance dataframes to manage millions of telemetry records efficiently in memory.
* **Master Parquet Creation**: Consolidates all host telemetry into a single Master Parquet file.
* **Optimized Compression**: Utilizes 'snappy' compression to balance fast read/write speeds with reduced storage footprint for the 3-year historical dataset.

### **4. Quality Audit & Exit Criteria**

Upon completion, the script performs an automated audit to verify data integrity and diversity.

* **Row-Count Parity**: Asserts that the final record count strictly equals the product of Total Hosts, Total Resources, and Total Days.
* **Behavioral Diversity**: Joins the generated telemetry back to host metadata to confirm that all 10 behavioral varieties (scenario/variant combinations) successfully survived the generation process.
* **Audit Reporting**: Generates a summary report detailing the total rows, file size, and behavioral variety count to confirm the system is ready for the forecasting stage.

---

### **System Requirements**

* **Entrance Criteria**: Initialized database with host and time dimensions.
* **Exit Criteria**: A master Parquet file with verified row-counts and behavioral representation.
* **Dependencies**: Relies on `duckdb` for metadata, `numpy` for mathematical synthesis, `polars` for data orchestration, and `tqdm` for progress tracking.