# API and Integration Guide: HorizonScale

## Overview

This guide provides technical documentation for integrating with the HorizonScale capacity management engine. It details the library utilities, data interfaces, and modular hooks required to extend the system or connect its outputs to downstream visualization and reporting tools.

---

## 1. Core Library Utilities (`/lib`)

The foundation of the framework resides in the utility layer, which provides standardized services across the entire pipeline.

* **Global Configuration (`config.py`):** Centralizes all environmental parameters, including server counts, historical look-back windows, and forecast horizons.
* **Centralized Logging (`logging.py`):** Provides a unified logging interface for tracking execution flow and troubleshooting parallel process errors.
* **Scenario Generation (`scenario_generators.py`):** Contains the logic for creating complex utilization patterns, available for use in testing new model variants.

## 2. Data Pipeline Interfaces

HorizonScale uses a file-based data exchange architecture optimized for high-speed I/O.

* **Input Layer:** Processes raw utilization data and configuration metadata.
* **Storage Layer (Parquet):** All intermediate and master datasets are stored in Apache Parquet format. Integration with external BI tools should utilize Parquet-compatible connectors for optimal performance.
* **Export Layer:** The pipeline generates monthly CSV summaries and risk-exception tables designed for ingestion by dashboarding platforms.

![alt text](image.png)
## 3. Extending the Forecasting Engine

The system is designed to be "pluggable," allowing for the introduction of new predictive algorithms.

* **Model Competition Hook:** The `model_competition.py` module acts as the orchestrator. New models (e.g., XGBoost, LSTM) can be integrated by adhering to the standard input/output interface defined in the pipeline.
* **Hyperparameter Injection:** Model-specific parameters can be injected via the central configuration library to allow for tuning without modifying core logic.

## 4. Downstream Integration

HorizonScale facilitates easy connection to enterprise reporting environments.

* **Risk Reporting Service:** Outputs from the risk reporting module provide granular "High Trust" indicators and 6-month outlooks in structured formats.
* **Dashboard Connectivity:** The `risk_dashboard.py` module serves as a reference implementation for visualizing capacity breaches and resource trends.
* **Alerting Hooks:** The generate process includes logic for notification triggers based on user-defined utilization thresholds and "Planned Action" dates.

## 5. System Requirements & Dependencies

* **Environment:** Python 3.x with a focus on `multiprocessing` and `concurrent.futures`.
* **Core Libraries:** Pandas, Prophet, PyArrow (for Parquet handling), and Scikit-learn.
* **Hardware:** Optimized for multi-core CPU environments to maximize the "Turbo" parallel processing capabilities.