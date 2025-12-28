"""
baseline_utils.py
Author: Sean L Girgis

Purpose of the script:
    This module provides shared utility functions specifically tailored for baseline forecasting models (Prophet, SARIMA, ETS) in the HorizonScale project.
    It centralizes common operations like data loading, forecast saving to database, plotting with confidence intervals, model evaluation,
    confidence interval extraction, and host/resource validation with fallbacks. These utilities ensure consistency across baseline scripts
    (e.g., 06_baseline_prophet.py, 07_baseline_sarima_ets.py, 08_baseline_comparison.py), handle DuckDB interactions uniformly,
    enforce success criteria through assertions and logging, and support headless plotting for CI/server environments.
    Key extensions include global warning suppression for statsmodels and re-export of load_data from db_utils for convenience.

What it does:
    - Re-exports load_data from db_utils for easy import in baseline scripts.
    - Sets up module-level logging with a dedicated file (baseline_utils.log) if not already configured.
    - Creates a baselines-specific plots subdirectory and ensures it exists.
    - Provides a setup_logging function for script-specific loggers (used in individual baseline scripts for dedicated logs).
    - Validates and falls back to sample hosts if requested host/resource combo is invalid.
    - Plots forecasts with shaded confidence intervals, timestamps, and saves to baselines dir.
    - Extracts CI arrays from Prophet-style DataFrames.
    - Saves forecast arrays with CIs to DuckDB 'baseline_forecasts' table, with idempotent create/insert/delete.
    Overall, it abstracts repetitive tasks for baselines, promotes robustness via assertions (e.g., row counts, column presence),
    and ensures consistent visualization/logging across models.

Dependencies:
    - External packages:
        - polars (pl): For DataFrame creation in save_forecast_to_db.
        - duckdb: For database connections, table creation, queries, and registrations.
        - matplotlib: Set to 'Agg' backend for headless plotting; uses pyplot for figure creation/saving.
        - logging: Standard library for logger configuration.
        - pathlib.Path: For path handling (logs, plots).
        - datetime.datetime: For parsing forecast start and generating date ranges.
        - pandas (pd): For date_range in future dates and DataFrame in CI extraction.
        - numpy (np): For array operations like column_stack in extract_ci_array.
        - sklearn.metrics: For mean_absolute_percentage_error and mean_squared_error in evaluate_model.
        - warnings: To suppress specific statsmodels UserWarning globally.
    - Project-internal:
        - horizonscale.config: Imports DB_PATH, LOG_DIR, LOG_LEVEL, PLOTS_DIR, FORECAST_START_DATE, FORECAST_HORIZON, SAMPLE_HOST_IDS.
        - horizonscale.db_utils: Imports load_data as db_load_data for re-export.
    No heavy ML dependencies here; focused on data I/O, plotting, and utils supporting baseline models.

Outputs or Results:
    - Module-level side effects: Suppresses statsmodels warning; configures logger if needed; creates plots_dir / 'baselines' if missing.
    - Function results:
        - load_data: Re-exported; returns pd.DataFrame from db_utils.
        - setup_logging(name: str): Returns a configured logging.Logger with dedicated file/stream handlers.
        - get_valid_host_resource(host_id, resource): Returns tuple (str, str) of validated/fallback host/resource; logs warnings.
        - plot_forecast(forecast, ci, host_id, model_type, resource): Saves PNG to baselines dir with timestamp; logs success.
        - evaluate_model(actual, forecast): Dict {'MAPE': float, 'RMSE': float}.
        - extract_ci_array(forecast_df): np.ndarray [n, 2] for lower/upper bounds; asserts columns.
        - save_forecast_to_db(forecast, ci, host_id, model_type, resource): Saves to 'baseline_forecasts'; asserts inserted rows == len(forecast); logs.
    - Files: Timestamped forecast plots in plots/baselines/ (e.g., YYYY-MM-DD_baseline_forecast_Prophet_server-xyz_cpu.png).
    - Database: Creates/inserts into 'baseline_forecasts' table (schema: ds DATE, yhat/yhat_lower/yhat_upper DOUBLE, host_id/resource/model_type VARCHAR).

What the code inside is doing:
    The module is structured with imports, global setups, re-exports, logger init, dir creation, then function definitions:
    - Imports: pl, duckdb, matplotlib (set 'Agg' backend for headless), plt, logging, Path, datetime, pd, np, sklearn metrics, warnings.
      From config: paths/constants; from db_utils: load_data.
    - warnings.filterwarnings: Globally ignores statsmodels UserWarning about frequency info.
    - load_data = db_load_data: Re-export for import convenience (avoids importing db_utils separately).
    - logger setup: Gets __name__ logger; if no handlers, ensures LOG_DIR, basicConfig with level/format, FileHandler (w mode), StreamHandler.
    - plots_dir = PLOTS_DIR / 'baselines'; mkdir exist_ok=True: Creates baselines subdir.
    - def setup_logging(name: str = __name__):
        - Similar to module logger but per-name; avoids duplicates; dedicated log_file = LOG_DIR / f"{name}.log"; returns logger.
    - def get_valid_host_resource(host_id, resource):
        - Connects to DB_PATH.
        - Checks if host/resource exists in processed_data (LIMIT 1).
        - If not, warns; tries fallback to SAMPLE_HOST_IDS with resource.
        - If fallback found, uses first; else queries distinct host/resource pairs, picks first.
        - Closes con; returns valid tuple; logs fallback if used.
    - def plot_forecast(forecast: np.ndarray, ci: np.ndarray, host_id, model_type, resource):
        - Generates future dates via pd.date_range from FORECAST_START_DATE, periods=FORECAST_HORIZON.
        - Creates fig/ax; plots forecast line.
        - Shades CI with fill_between (alpha=0.3).
        - Sets title/labels/grid; saves via save_plot with name f'baseline_forecast_{model_type}_{host_id}_{resource}'.
        - Logs success.
    - def evaluate_model(actual: np.ndarray, forecast: np.ndarray) -> dict:
        - Computes MAPE and RMSE using sklearn functions; returns dict.
    - def extract_ci_array(forecast_df: pd.DataFrame) -> np.ndarray:
        - Checks for required 'yhat_lower', 'yhat_upper' columns; raises if missing.
        - Stacks values as [n, 2] array via np.column_stack.
    - def save_forecast_to_db(forecast: np.ndarray, ci: np.ndarray, host_id, model_type, resource='cpu'):
        - Parses FORECAST_START_DATE to datetime; generates future_dates range.
        - Creates pl.DataFrame with ds, yhat, yhat_lower/upper (from ci), repeated host/resource/model.
        - Connects to DB; registers as 'forecast_temp'.
        - Creates baseline_forecasts table if not exists (specified schema).
        - Deletes existing rows for this host/resource/model (idempotent).
        - Inserts from temp.
        - Queries count for this combo; asserts == len(forecast); logs success; closes con.
    Code focuses on reliability: assertions for criteria, logging everywhere, defensive checks (e.g., duplicates, existence).
    Truncated parts suggest more, but based on provided, it's comprehensive for baseline support.
"""