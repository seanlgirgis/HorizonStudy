docs/lib/utils.md

### **File Overview: `utils.py**`

The `utils.py` module centralizes shared utility functions for the HorizonScale project, covering path detection, visualization persistence, and model performance metrics. It serves as a foundational layer to ensure mathematical and operational consistency across the entire 2,000-server capacity pipeline.

---

### **1. Environment & Path Utilities**

This section manages the project's spatial awareness to ensure scripts function correctly regardless of the execution environment.

* **Dynamic Root Detection**: The `get_project_root` function automatically identifies the base directory of the project.
* **Execution Context**: It handles execution from various subdirectories, such as libraries or scripts, ensuring that relative file paths remain valid.

### **2. Visualization Utilities**

Standardized methods for managing graphical output are provided to support data analysis and reporting.

* **Persistence Management**: The `save_plot` function saves figures using an enterprise-standard timestamp prefix to ensure organized tracking of results.
* **Memory Optimization**: To prevent memory leaks during batch visualization tasks, the utility automatically closes figures after they are written to disk, freeing up system resources.
* **Validation**: It includes error handling and logging to verify that plot files are successfully persisted before completing a task.

### **3. Analytical & Forecasting Metrics**

This layer provides the mathematical tools necessary to evaluate the accuracy of the forecasting engine.

* **Model Evaluation**: The `evaluate_model` function calculates standard forecasting metrics, specifically Mean Absolute Percentage Error (MAPE) and Root Mean Squared Error (RMSE).
* **Descriptive Profiles**: The `compute_utilization_stats` utility generates rapid statistical profiles of utilization series, including mean, standard deviation, and maximum values.

### **4. Temporal Helpers**

Ensuring referential integrity for time-series data is critical for the synthetic data and forecasting layers.

* **Calendar Generation**: The `generate_time_dimensions` function creates a contiguous daily calendar between specified start and end dates.
* **Database Alignment**: It produces formatted strings (YYYYMM) used for seeding databases and aligning telemetry records across the three-year historical window.

---

### **System Requirements**

* **Entrance Criteria**: Requires access to project configuration for path resolution and initialization of the centralized logging framework.
* **Exit Criteria**: Provides verified, idempotent methods for data persistence and ensures 100% referential integrity for date generation.
* **Dependencies**: Relies on `numpy`, `pandas`, `matplotlib`, and `sklearn` for mathematical and visualization operations.