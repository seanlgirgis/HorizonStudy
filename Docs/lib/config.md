docs/lib/config.md

### **File Overview: `config.py**`

The `config.py` file serves as the **Centralized Configuration Engine** for the HorizonScale capacity management pipeline. It centralizes all environmental paths, behavioral distributions (the "Mathematical DNA"), and forecasting parameters required to manage a fleet of **2,000 servers**.

---

### **1. Directory & Path Architecture**

HorizonScale utilizes a dynamic path resolution system based on `pathlib` to ensure environmental portability.

* **`PROJECT_ROOT`**: Resolves the base directory relative to the file location.
* **Database & Metadata**: Defines paths for the DuckDB synthetic database (`horizonscale_synth.db`) and SQL schema directories.
* **Telemetry Persistence**: Sets the location for the **Master Parquet** file, which stores daily utilization data from 2023 through 2025.
* **Legacy Simulation**: Includes paths for exporting monthly CSVs to maintain compatibility with downstream reporting tools.

### **2. Generation & Behavioral DNA**

This section defines how the pipeline simulates high-fidelity utilization data across a three-year historical window.

* **`DEFAULT_NUM_HOSTS`**: Scaled to **2,000 servers** to test high-performance processing.
* **`Scenario` Enum**: Standardizes server behavior types including `STEADY_GROWTH`, `SEASONAL`, `BURST`, `LOW_IDLE`, and `CAPACITY_BREACH`.
* **`SCENARIO_DISTRIBUTION`**: Assigns specific "market shares" to behaviors, such as 35% for `STEADY_GROWTH` and 5% for high-risk `CAPACITY_BREACH` events.
* **`VARIANT_WEIGHTS`**: Distinguishes between "common" (80%) and "rare" (20%) risk profiles within each scenario to ensure data variety.

### **3. Mathematical DNA (Generator Configuration)**

Defines the technical specifics for the data generation layer.

* **Base Ranges**: Initial utilization percentages (e.g., 35–55% for normal growth).
* **Growth & Noise**: Sets the total growth over three years and the "noise" or random jitter applied to daily telemetry.
* **Seasonality**: Defines the magnitude of weekly cycles to ensure realistic forecasting patterns.

### **4. Metadata & Resource Mapping**

Standardizes how telemetry is categorized across different organizational and technical boundaries.

* **Resource Types**: Maps specific attributes for `cpu`, `memory`, `disk`, and `network`, including capacity fields (e.g., `max_cores`) and adjustment factors.
* **Organizational Context**: Defines standard regions (`na`, `emea`, `latam`, `asiapac`) and departments (`Retail Banking`, `Wealth Management`, etc.) for enterprise reporting.

### **5. Forecasting Parameters**

Establishes the foundation for the forecasting competition engine.

* **`FORECAST_START_DATE`**: Sets the initiation point for projections as 2025-12-01.
* **`FORECAST_HORIZON`**: Defines a **6-month projection** (180 days) window.
* **Confidence Logic**: Supports the **"High Trust" 3-month status** vs. the full 6-month outlook.