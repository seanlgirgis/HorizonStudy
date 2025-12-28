"""
db_utils.py
Author: Sean L Girgis

Purpose of the script:
    This module contains utility functions for interacting with the DuckDB database in the HorizonScale project.
    It provides standardized methods for loading and saving data related to processed time series, historical values,
    forecasts, and metrics. Extended from basic load/save to include full processed data with rolling features,
    fallback loading from Parquet, and specific loaders for baselines/advanced models. These utilities ensure
    consistent database access, error handling, and data validation across the pipeline, reducing duplication
    in data ingestion/export code for EDA, forecasting, comparison, and anomaly detection scripts.

What it does:
    - Loads filtered processed data (ds/y columns) for a specific host/resource as Pandas DataFrame for modeling.
    - Loads full processed data (all columns) with DuckDB priority and Parquet fallback, applying defensive renames
      and assertions for required columns.
    - Loads historical data (similar to load_data but reusable).
    - Loads stored forecasts for multiple model types from the 'baseline_forecasts' table.
    - Saves computed metrics (MAPE/RMSE) to a 'model_comparisons' table with timestamps.
    - Loads full processed data including rolling mean/std features, with column renames and validation.
    Overall, it abstracts DuckDB connections, queries, registrations, and Polars/Pandas conversions, while enforcing
    success criteria via assertions and logging to make data operations robust and traceable.

Dependencies:
    - External packages:
        - polars (pl): For efficient DataFrame operations, especially loading from DuckDB and Parquet.
        - duckdb: Core library for in-memory/local file database connections and SQL execution.
        - pandas (pd): For DataFrame conversions where models (e.g., Prophet, Statsmodels) expect Pandas input.
        - numpy (np): Used in array stacking for confidence intervals.
    - Standard library: datetime.datetime for timestamping metrics.
    - Project-internal:
        - horizonscale.config: Imports DB_PATH, PROCESSED_PARQUET_FILE, PROCESSED_REQUIRED_COLS, MODEL_TYPES,
          FORECAST_HORIZON, FORECAST_START_DATE for paths and constants.
        - horizonscale.utils: Imports setup_logging to initialize the module's logger.
    Designed to be imported in baseline_utils, forecasting scripts (06-10), EDA (04-05), and comparisons (08).

Outputs or Results:
    - No direct file outputs; focuses on in-memory data loading/saving to DuckDB tables.
    - Key function results:
        - load_data: pd.DataFrame with 'ds' (datetime) and 'y' (float) columns; raises if empty.
        - load_processed_data: pl.DataFrame with all processed columns (date parsing if needed); asserts >0 rows and required cols.
        - load_historical_data: pd.DataFrame similar to load_data (aliased for clarity in comparisons).
        - load_forecasts: Dict of model_type to pd.Series (yhat values) for the forecast horizon.
        - save_metrics_to_db: Saves to 'model_comparisons' table; asserts row count matches input metrics.
        - load_full_processed_data: pd.DataFrame with 'ds', 'y', 'rolling_mean_7', 'rolling_std_7'; renames and asserts columns.
    - Database side effects: Creates tables if needed (baseline_forecasts, model_comparisons); inserts/deletes data.
    - Logging: Info/warnings/errors for loads, saves, fallbacks, and failures.

What the code inside is doing:
    The module starts with imports, logger setup, then defines functions with docstrings and logic:
    - Imports: pl, duckdb, pd, np, datetime; config constants; utils logging.
    - logger = setup_logging(__name__): Initializes module-specific logger.
    - load_data(host_id: str = default, resource: str = 'cpu') -> pd.DataFrame:
        - Connects to DB_PATH.
        - Executes SQL to select ds/y filtered by host/resource, ordered by ds.
        - Fetches as Pandas DF via fetchdf().
        - Closes connection.
        - Asserts non-empty; converts ds to datetime; logs row count.
    - load_processed_data() -> pl.DataFrame:
        - Tries DuckDB SELECT * FROM processed_data as Polars DF via .pl().
        - On failure, warns and falls back to pl.read_parquet(PROCESSED_PARQUET_FILE).
        - Renames legacy columns (date->ds, utilization->y, rolling_mean_7->y_roll_mean, etc.) if present.
        - Parses ds to date if string.
        - Asserts >0 rows and all PROCESSED_REQUIRED_COLS present; logs source and rows.
    - load_historical_data(host_id, resource) -> pd.DataFrame:
        - Wrapper around load_data; same logic but named for historical context in comparisons.
    - load_forecasts(host_id, resource) -> dict[str, pd.Series]:
        - Connects to DB.
        - For each MODEL_TYPE, queries baseline_forecasts for yhat, filtered by host/resource/model.
        - Fetches as DF, extracts yhat as Series.
        - Asserts length == FORECAST_HORIZON; logs per model.
        - Returns dict of model_type: yhat Series.
    - save_metrics_to_db(metrics: pd.DataFrame, host_id, resource):
        - Connects to DB.
        - Converts metrics to Polars, adds host_id, resource, timestamp columns.
        - Registers as temp table.
        - Creates model_comparisons table if not exists (VARCHAR/DOUBLE/TIMESTAMP schema).
        - Inserts from temp.
        - Queries count for this host/resource/timestamp; asserts matches len(metrics).
        - Logs success; closes.
    - load_full_processed_data(host_id, resource) -> pd.DataFrame:
        - Connects to DB.
        - Queries ds, y, y_roll_mean, y_roll_std filtered/ordered.
        - Fetches as Pandas DF.
        - Closes.
        - Asserts non-empty.
        - Renames rolling cols to rolling_mean_7/rolling_std_7.
        - Asserts required columns present.
        - Converts ds to datetime; logs rows with features mention.
    Code emphasizes defensive programming: try/except for fallbacks, assertions for criteria, consistent connect/close,
    and logging for traceability. Truncated parts in the provided code suggest more functions, but based on description,
    it's extended for various load/save needs.
"""