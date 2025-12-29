# README.md: Project Entry Point

## HorizonScale: AI-Driven Capacity Management

HorizonScale is a modern, high-performance capacity utilization and forecasting pipeline designed to predict resource utilization across large-scale server estates. By evolving beyond legacy distributed computing constraints, this system utilizes advanced time-series modeling and parallel processing to deliver high-fidelity resource forecasts for 2,000+ individual server nodes.

## Key Features

* 
**Scalable Architecture**: Optimized to handle thousands of servers simultaneously without the latency of traditional big-data frameworks.


* 
**Turbo Forecasting**: Bypasses the Python Global Interpreter Lock (GIL) via multiprocessing to process CPU-bound forecasting tasks in parallel across all available system cores.


* 
**Confidence Scoring**: Implements a "High Trust" status assigned to the first 3 months of a 6-month forecast window to drive immediate operational decisions.


* 
**Model Competition**: Automatically evaluates and selects the most accurate algorithm (Prophet, XGBoost, or LSTM) for each unique resource profile.



## Project Documentation

Detailed information regarding the system's design, performance, and operation can be found in the following documents:

* 
**[Architecture Evolution](https://github.com/seanlgirgis/HorizonStudy/blob/main/Docs/TREND_TO_HORIZONSCALE_EVOLUTION.md)**: Details the transition from legacy "stub and module" environments to modern Python-native pipelines.


* 
**[Performance Benchmark](https://github.com/seanlgirgis/HorizonStudy/blob/main/Docs/PERFORMANCE_BENCHMARK_REPORT.md)**: Quantitative analysis of the multiprocessing "Turbo" speed gains over legacy sequential methods.


* 
**[Testing & Validation](https://github.com/seanlgirgis/HorizonStudy/blob/main/Docs/TESTING_AND_VALIDATION_STRATEGY.md)**: Overview of statistical backtesting, data integrity gates, and the logic behind the "High Trust" window.


* 
**[Operational Runbook](https://github.com/seanlgirgis/HorizonStudy/blob/main/Docs/OPERATIONAL_RUNBOOK.md)**: Step-by-step execution guide and command-line reference for the synthetic and pipeline layers.


* 
**[Integration Guide](https://github.com/seanlgirgis/HorizonStudy/blob/main/Docs/API_AND_INTEGRATION_GUIDE.md)**: Documentation for core utilities in `/lib` and instructions for extending the model competition engine.


* 
**[Future Roadmap](https://github.com/seanlgirgis/HorizonStudy/blob/main/Docs/FUTURE_ROADMAP.md)**: Strategic plans for GPU acceleration, deep learning integration, and distributed computing.

* 
**[Lessons Learned](https://github.com/seanlgirgis/HorizonStudy/blob/main/Docs/lessons_learnt.md)**: Lessons learnt. key technical and strategic insights gained during the development of the HorizonScale platform.


## Quick Start

To initialize the environment and launch the risk dashboard, execute the following commands in sequence:

```bash
# Initialize the local database foundation
python3 src/HorizonScale/synthetic/00_init_db.py

# Execute the high-performance Turbo forecasting engine
python3 src/HorizonScale/pipeline/06_turbo_prophet.py

# Launch the interactive visual interface
streamlit run src/HorizonScale/pipeline/10_risk_dashboard.py

```
