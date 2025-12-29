docs/pipeline/09_risk_reporting.md

### **File Overview: `09_risk_reporting.py**`

The `09_risk_reporting.py` script serves as the final analytical layer of the HorizonScale pipeline. Its mission is to act as a balanced analyst by scanning the champion dataset to identify capacity breaches across the 2,000-server fleet. It prioritizes significant risks and automates the generation of a visual evidence gallery to support executive capacity planning.

---

### **1. Analytical Risk Logic**

The script employs a multi-tiered approach to identify and categorize potential capacity exhaustion:

* **Risk Identification**: Scans the champion forecast dataset for any resource series where the upper confidence bound (`yhat_upper`) is projected to meet or exceed 95% utilization.
* **Priority Flagging (⭐)**: Implements a prioritization engine that flags high-interest risks. A priority status is assigned if a forecast exhibits high volatility (standard deviation > 2) or if the projected peak utilization exceeds 105%.
* **Statistical Profiling**: For every identified risk, the script calculates the earliest projected breach date, the total forecast volatility, and the expected peak impact.

### **2. Visual Evidence Gallery**

To provide transparency for the identified risks, the script automates a large-scale visualization workflow:

* **High-Resolution Plots**: Generates PNG charts for every host and resource identified as at-risk, displaying the forecast trend, confidence intervals, and the 95% critical threshold.
* **Automated Scaling**: Dynamically adjusts plot boundaries to accommodate severe breaches while maintaining a clear view of the historical-to-future transition.
* **Persistence**: Organizes all generated images into a dedicated `risk_visuals` directory for easy inclusion in stakeholder reports.

### **3. Reporting and Distribution**

The orchestration logic provides a high-level summary of the fleet's risk profile:

* **Model-Specific Distribution**: Generates a report detailing how many risks were identified by each winning model (e.g., Prophet vs. XGBoost) and their associated priority status.
* **Master Risk Inventory**: Persists the identified risks into a structured `capacity_risks` table within the database, serving as the source of truth for downstream dashboards.
* **Relational Integration**: Utilizes DuckDB views to seamlessly join forecasting results with host metadata for comprehensive reporting.

### **4. Technical Execution**

* **Memory Efficiency**: Processes plots sequentially and closes figure objects immediately after disk persistence to prevent memory exhaustion during the generation of hundreds of images.
* **Performance Monitoring**: Uses a centralized execution timer to track the duration of the analysis and plotting phases, ensuring the reporting layer remains performant.
* **Path Management**: Employs standardized directory resolution to ensure the visual gallery is correctly placed within the project's data hierarchy.

---

### **System Requirements**

* **Entrance Criteria**: A finalized champion dataset in Parquet format and a populated host database.
* **Exit Criteria**: A populated `capacity_risks` table and a visual gallery of PNG forecast plots.
* **Dependencies**: Relies on `duckdb` for risk identification, `pandas` for data structuring, and `matplotlib` for automated visualization.