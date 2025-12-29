docs/synthetic/00_init_db.md

### **File Overview: `00_init_db.py**`

The `00_init_db.py` script initializes the physical and relational foundation of the HorizonScale pipeline. It establishes the DuckDB database, defines the core schema via external DDL, and seeds the initial inventory of **2,000 servers** with distinct behavioral "DNA". This step ensures a standardized, deterministic starting point for the entire 3-year capacity simulation.

---

### **1. Database & Schema Initialization**

The script manages the lifecycle of the local data environment to ensure idempotency and structural integrity.

* **Idempotency Layer**: Automatically purges any existing database files at the defined path to prevent data pollution between runs.
* **Schema Definition**: Executes an external `create_tables.sql` DDL file to initialize the relational tables, including `hosts`, `hierarchy`, and `time_periods`.
* **Relational Foundation**: Establishes the tables necessary for SQL joins during the telemetry generation and forecasting stages.

### **2. Inventory Seeding & Behavioral DNA**

This component generates the server fleet and assigns metadata that dictates how each server will behave in subsequent pipeline stages.

* **Weighted Distribution**: Uses weighted probabilities to assign each host a specific scenario (e.g., `SEASONAL`) and a risk variant (e.g., `EXTREME`) based on enterprise-wide distribution settings.
* **Asset Metadata**: Assigns unique UUIDs to each node and populates hardware attributes, including CPU cores, memory capacity, and storage limits.
* **Organizational Context**: Randomly distributes hosts across various regions and departments to support realistic global risk reporting.

### **3. Temporal Dimensioning**

To support time-series analysis, the script builds a contiguous daily calendar dimension.

* **Simulation Window**: Generates a daily range of dates covering the full 3-year historical foundation (2023–2025).
* **Temporal Mapping**: Creates `yearmonth` identifiers to facilitate partitioning and alignment of utilization telemetry in later stages.

### **4. Validation & Exit Audit**

The script concludes with a rigorous audit to verify that the initialization meets all operational requirements.

* **Behavioral Audit**: Aggregates the seeded inventory by scenario and variant to ensure all 10 behavioral combinations are present in the fleet.
* **Record Verification**: Confirms that the total host count matches the target fleet size of 2,000 servers.
* **Success Criteria**: If all DNA profiles are present and the host count is accurate, the system logs the "Genesis Complete" status, signaling the database is ready for telemetry.

---

### **System Requirements**

* **Entrance Criteria**: Requires a valid `create_tables.sql` file and behavioral distribution weights defined in the project configuration.
* **Exit Criteria**: A physically created `horizonscale_synth.db` on disk containing exactly 2,000 seeded host records.
* **Dependencies**: Relies on `duckdb` for the database engine, `polars` for fast data manipulation, and `faker` for synthetic metadata generation.