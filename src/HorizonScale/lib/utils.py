"""
utils.py
Author: Sean L Girgis

Purpose of the script:
    This module provides a collection of shared utility functions used throughout the HorizonScale project.
    It centralizes common operations for logging setup, project path detection, plot saving, model evaluation metrics,
    and helper functions for date generation and time series statistics. These utilities promote code reuse, consistency,
    and maintainability across data generation, processing, EDA, forecasting, and visualization scripts/notebooks.
    By importing this module, other components avoid duplicating boilerplate code for logging, plotting, and basic metrics.

What it does:
    - Configures per-script logging with dedicated log files (overwritten each run) and console output.
    - Dynamically detects the project root directory, handling differences between script execution and Jupyter notebooks.
    - Safely saves Matplotlib figures with timestamps, ensures directories exist, closes figures to free memory,
      and raises errors on failure for robust pipeline execution.
    - Computes standard forecasting evaluation metrics: MAPE (Mean Absolute Percentage Error) and RMSE (Root Mean Squared Error).
    - Generates contiguous daily dates and corresponding year-month strings for a given range of years (used in synthetic data setup).
    - Calculates basic descriptive statistics (mean, std, max) for a utilization time series.
    Overall, it ensures consistent behavior for cross-cutting concerns like logging, paths, plotting, and simple analytics,
    while enforcing success criteria (e.g., plot save assertions) to make pipelines more reliable and debuggable.

Dependencies:
    - Standard library: logging, pathlib.Path, os, datetime.datetime, datetime.timedelta, calendar.monthrange
    - External packages:
        - matplotlib.pyplot: For figure handling and closing in save_plot.
        - numpy: For array operations in evaluate_model and compute_series_stats.
        - scikit-learn (sklearn.metrics): Provides mean_absolute_percentage_error and mean_squared_error implementations.
    - Project-internal: Imports LOG_DIR, LOG_LEVEL, PLOTS_DIR from horizonscale.config for centralized path/level settings.
    No heavy dependencies; designed to be lightweight and importable early in the pipeline.

Outputs or Results:
    - This is a utility module with no direct runtime outputs (e.g., files/database changes when imported alone).
    - Key "outputs" are the functions themselves, which produce:
        - setup_logging: A configured logging.Logger instance with file (logs/<name>.log) and stream handlers.
        - get_project_root: A pathlib.Path object pointing to the project root (critical for relative path resolution in notebooks/scripts).
        - save_plot: Saves a PNG file to PLOTS_DIR with format '{timestamp}_{name}.png'; logs success or raises AssertionError on failure.
        - evaluate_model: Returns a dict {'MAPE': float, 'RMSE': float} for model performance assessment.
        - generate_dates_and_yearmonths: Tuple of two lists – datetime objects (daily dates) and strings (YYYYMM yearmonths).
        - compute_series_stats: Dict with 'mean_util', 'std_util', 'max_util' floats for a given series.
    - Indirect results: Enables consistent logging across scripts (dedicated per-script logs), timestamped plots in /plots/,
      reliable metric computation for baselines/advanced models, and proper date ranges for data generation.

What the code inside is doing:
    The module is structured as imports followed by function definitions, with no executable code at module level:
    - Imports: Standard libs (logging, Path, os, datetime), external (matplotlib.pyplot as plt, numpy as np, sklearn metrics),
      and project config constants (LOG_DIR, LOG_LEVEL, PLOTS_DIR).
    - setup_logging(name: str):
        - Gets or creates a logger by name.
        - Checks for existing handlers to prevent duplicates.
        - Ensures LOG_DIR exists.
        - Configures basicConfig with INFO level (or from config), custom timestamp format,
          FileHandler (overwrite mode='w' for clean runs), and StreamHandler for console.
        - Returns the logger.
    - get_project_root() -> Path:
        - Gets current working directory via os.getcwd().
        - If cwd basename is 'notebooks', returns parent (assumes notebooks/ subdir); else returns cwd.
        - Handles execution context differences gracefully.
    - save_plot(fig, name: str):
        - Ensures PLOTS_DIR exists.
        - Generates timestamped filename (YYYY-MM-DD_name.png).
        - Saves fig via fig.savefig() to full path.
        - Closes figure to prevent memory leaks.
        - Asserts file existence post-save; raises AssertionError if missing.
        - Logs save success using module's logger.
    - evaluate_model(actual: np.ndarray, forecast: np.ndarray) -> dict:
        - Computes MAPE via sklearn's mean_absolute_percentage_error.
        - Computes RMSE as sqrt of mean_squared_error.
        - Returns dict with both metrics.
    - Additional helpers section:
        - Re-imports datetime/timedelta and monthrange (for clarity).
        - Imports numpy again (redundant but explicit).
        - generate_dates_and_yearmonths(years: range) -> tuple[list[datetime], list[str]]:
            - Nested loops over years/months/days using monthrange for accurate day counts.
            - Builds lists of datetime objects and formatted '%Y%m' strings.
            - Returns both for use in time dimension seeding.
        - compute_series_stats(series: np.ndarray) -> dict:
            - Simple dict with np.mean, np.std, np.max of the input array.
            - Used for quick utilization summaries during generation/validation.
    The code emphasizes robustness (idempotency, assertions, duplicate prevention) and is designed for heavy reuse
    across the pipeline without side effects on import.
"""

import logging
import sys
from horizonscale.lib.config import LOG_DIR, LOG_LEVEL

def setup_logging(script_name: str):
    """
    Initializes the ROOT logger. Call this ONLY once in your main script.
    
    A) All code writes to one file.
    B) Console (INFO/WARNING) vs File (DEBUG/INFO).
    C) Named after the script (e.g., 00_init_db.log).
    D) Overwrites existing file (mode='w').
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{script_name}.log"
    
    # 1. Get the Root Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture everything at the root level

    # Prevent adding multiple handlers if setup is called twice
    if not root_logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # 2. File Handler (Lower level: DEBUG/INFO)
        # Mode='w' ensures it starts fresh/overwrites
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setLevel(logging.DEBUG) 
        file_handler.setFormatter(formatter)

        # 3. Stream/Console Handler (Higher level: INFO or WARNING)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO) 
        console_handler.setFormatter(formatter)

        # 4. Add handlers to root
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

    return logging.getLogger(script_name)