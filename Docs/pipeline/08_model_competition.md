docs/pipeline/08_model_competition.md

### **File Overview: `08_model_competition.py**`

The `08_model_competition.py` script serves as the **Tournament Layer** of the HorizonScale pipeline. Its primary role is to act as the objective arbiter between different forecasting engines (Prophet vs. XGBoost). It conducts a rigorous backtest audit to select the "Champion" model for each individual asset based on historical accuracy during a specific hold-out period.

---

### **1. Tournament Logic & Audit Phase**

The script implements a competitive framework to determine the most reliable forecasting model for every asset in the 2,000-server fleet.

* **Backtest Audit**: Compares predictions from each model against real historical values for a designated 4-month hold-out set.
* **KPI Metric**: Uses Mean Absolute Percentage Error (MAPE) as the primary key performance indicator to measure accuracy.
* **Leaderboard Generation**: Calculates the MAPE for over 16,000 series (combining multiple models and resources) and ranks them to identify the superior engine for each host/resource pair.

### **2. Champion Selection & Extraction**

Once the audit is complete, the script transitions from evaluation to the selection of production-ready data.

* **Winning Model Assignment**: Assigns "Champion" status to the model with an accuracy rank of 1 (lowest MAPE).
* **Forecast Consolidation**: Merges the winning model's forecasts—including predictions and confidence bounds—into a final unified dataset.
* **Schema Alignment**: Employs explicit column selection and union logic to ensure data consistency across different modeling sources, preventing errors during large-scale database joins.

### **3. Persistence and Performance**

The final output is optimized for high-speed downstream consumption by reporting and dashboarding tools.

* **Production Parquet**: Exports the consolidated winning forecasts into a final "Champion" Parquet file using high-performance storage formats.
* **Relational Integration**: Maintains the final champion forecasts in the DuckDB environment for immediate analytical access.
* **Idempotency**: Features logic to reset the leaderboard and final forecast tables at the start of each run to ensure a fresh audit.

### **4. Analytical Standings**

The script provides a summary of the tournament outcomes to give architects visibility into model performance.

* **Model Win Rates**: Reports the number of servers won by each model type (Prophet vs. XGBoost).
* **Accuracy Benchmarking**: Calculates the average MAPE for the winning models to provide an overall assessment of the pipeline's forecasting health.
* **Performance Auditing**: Tracks the total duration of the audit and selection phases to monitor for processing bottlenecks.

---

### **System Requirements**

* **Entrance Criteria**: Successful completion of the Prophet and XGBoost modeling stages with results persisted in Parquet or DuckDB tables.
* **Exit Criteria**: A finalized champion dataset persisted to both Parquet and the database, with 100% of hosts assigned a winning model.
* **Dependencies**: Relies on `duckdb` for complex SQL auditing, `polars` for data persistence, and the project's central configuration for data paths.