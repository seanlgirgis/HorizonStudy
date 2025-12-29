docs/pipeline/04_baseline_forecasting.md

### **File Overview: `04_baseline_forecasting.py**`

The `04_baseline_forecasting.py` script serves as the **Forecasting Laboratory**. Its purpose is to select a representative cross-section of the 2,000-server fleet and project utilization trends 180 days into the future. By using a specialized forecasting engine with physical constraints, the script validates the pipeline's ability to handle diverse behavioral profiles.

---

### **1. Entrance Audit and Genesis Criteria**

The forecasting process requires a verified historical data foundation before execution.

* **Analytical Base Table (ABT) Verification**: Confirms the existence of the refined `processed_data` table produced by the previous refinery stage.
* **Inventory Readiness**: Requires access to the master host inventory to identify and scout specific behavioral targets for modeling.
* **Environmental Pass**: Aborts execution if the prerequisite refinery data is missing to ensure mathematical consistency.

### **2. Behavioral Scouting and Target Selection**

The script employs a scouting mechanism to ensure the laboratory results cover the full spectrum of the environment's diversity.

* **Modeling Targets**: Identifies 10 unique host UUIDs, selecting exactly one representative from each behavioral variety (e.g., Seasonal-Extreme, Steady Growth-Normal).
* **Resource Selection**: Randomly assigns a target resource, such as CPU or Memory, for each selected host to test multi-resource projection capabilities.
* **DNA Parity**: Ensures that the modeling outcomes reflect the different risk profiles established during the initial database seeding.

### **3. Batch Forecasting Lifecycle**

The core orchestration logic manages the training and projection phases for each target host.

* **Prophet Integration**: Utilizes the Facebook Prophet engine to model utilization trends, incorporating yearly and weekly seasonality.
* **Logistic Growth Constraints**: Implements a logistic growth model with physical boundaries, ensuring all projections are strictly capped at 100% and floored at 0%.
* **Future Grid Generation**: Creates a 180-day (6-month) projection horizon beyond the three-year historical baseline.
* **Idempotent Persistence**: Stores forecast results (predicted values and confidence bounds) in a dedicated table, using a "clean and insert" pattern to maintain a single source of truth for each host.

### **4. Visualization and Exit Criteria**

The laboratory outputs both data and visual evidence to verify the success of the projections.

* **Forecast Gallery**: Exports 10 standardized PNG visualizations to the plots directory, allowing for manual auditing of the model's accuracy against historical trends.
* **Physical Envelope Validation**: Ensures that all projected data points observe the physically realistic bounds of resource utilization.
* **System Reporting**: Logs a successful shutdown only after all projections are persisted and visualization artifacts are successfully exported.

---

### **System Requirements**

* **Entrance Criteria**: Populated `processed_data` and `hosts` tables in the database.
* **Exit Criteria**: Persisted forecast records and 10 representative visualization images.
* **Dependencies**: Relies on `duckdb` for data retrieval, `pandas` for data structuring, `prophet` for time-series modeling, and `matplotlib` for visualization.