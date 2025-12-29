docs/pipeline/03_data_pipeline.md

### **File Overview: `03_data_pipeline.py**`

The `03_data_pipeline.py` script serves as the **Refinery** of the project. Its primary function is to ingest fragmented, resource-specific monthly CSV files, standardize the telemetry, and unify it into a structured Analytical Base Table (ABT). This script ensures that data is cleaned and prepared specifically for the forecasting competition engine.

---

### **1. Entrance Audit and Genesis Criteria**

The refinery process only commences once the environment is verified for data readiness.

* **Legacy Source Verification**: Confirms that the directory of partitioned CSV files exists and contains the expected number of source files.
* **Relational Context**: Requires an initialized database containing the master host inventory to enable behavioral auditing.
* **Input Validation**: Ensures that the previous fragmentation stage completed successfully before warming up the ingestion engine.

### **2. ETL Engine (Scrub, Plug, and Enrich)**

The core logic orchestrates a multi-step Extraction, Transformation, and Loading (ETL) lifecycle designed for massive telemetry streams.

* **Dynamic Ingestion**: Utilizes high-speed CSV readers to glob and process all monthly files across various resource types (CPU, Memory, Disk, Network).
* **Schema Standardization**: Normalizes raw data into a specific format required for time-series modeling, specifically creating `ds` (datestamp) and `y` (target value) columns.
* **Timestamp Normalization**: Casts raw date strings into formal timestamp objects to ensure temporal alignment across the 3-year history.
* **Landing Zone Management**: Employs idempotent logic to reset and rebuild the target `processed_data` table, preventing data duplication.

### **3. Quality Audit and Exit Criteria**

After ingestion, the script performs a rigorous quality audit to verify that the refinery preserved the integrity of the synthetic dataset.

* **Integrity Join**: Executes a relational join between the newly processed telemetry and the original host inventory.
* **Behavioral Retention Audit**: Verifies that 100% of the 10 behavioral DNA varieties (scenario/variant combinations) survived the ingestion and transformation process.
* **Audit Summary**: Reports the total row count of the Analytical Base Table and confirms that the referential integrity of the host fleet is intact.

### **4. Technical Execution**

* **Standardized Format**: Populates the database with standardized columns ready for plug-and-play use in Prophet and other machine learning models.
* **Metadata Enrichment**: Injects resource-type identifiers and capacity limits back into the refined telemetry.
* **System Readiness**: Signals a successful shutdown only when the data is fully verified and ready for the baseline forecasting stage.

---

### **System Requirements**

* **Entrance Criteria**: Partitioned legacy CSVs and an initialized host inventory database.
* **Exit Criteria**: A populated `processed_data` table with 100% behavioral variety retention and standardized timestamp columns.
* **Dependencies**: Relies on `duckdb` for high-performance SQL processing and `polars` for lightweight data handling.