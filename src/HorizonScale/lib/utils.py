"""
utils.py
Author: Sean L. Girgis

Purpose:
    Centralizes shared utility functions for the HorizonScale project, covering 
    path detection, visualization persistence, and model performance metrics. 
    It ensures mathematical and operational consistency across the pipeline.

Genesis (Entrance Criteria):
    - Requires access to horizonscale.config for path resolutions.
    - Requires horizonscale.lib.logging for centralized event tracking.

Success (Exit Criteria):
    - Provides verified, idempotent methods for saving plots and calculating metrics.
    - Ensures 100% referential integrity for date and year-month generation.
    - Prevents memory leaks during batch visualization via automated figure disposal.
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, List, Dict
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error

from horizonscale.lib.config import LOG_DIR, PLOTS_DIR
from horizonscale.lib.logging import init_root_logging

# Initialize logger for the utility module itself
logger = init_root_logging("utils_module")

# =============================================================================
# 1. ENVIRONMENT & PATH UTILITIES
# =============================================================================

def get_project_root() -> Path:
    """
    Dynamically detects the project root directory. 
    Handles execution from scripts or Jupyter notebooks located in subdirectories.
    """
    cwd = Path(os.getcwd())
    # If executing from a /notebooks or /scripts subfolder, return the parent
    if cwd.name in ['notebooks', 'scripts', 'lib']:
        return cwd.parent
    return cwd

# =============================================================================
# 2. VISUALIZATION UTILITIES
# =============================================================================

def save_plot(fig: plt.Figure, name: str):
    """
    Saves a Matplotlib figure with an enterprise-standard timestamp prefix.
    
    Exit Criteria:
        - Directory existence is verified.
        - Figure is closed after saving to free system memory.
        - File existence is asserted post-operation.
    """
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    filename = f"{timestamp}_{name}.png"
    save_path = PLOTS_DIR / filename
    
    try:
        fig.savefig(save_path, bbox_inches='tight')
        plt.close(fig)  # Crucial for memory management in batch processing
        
        if not save_path.exists():
            raise FileNotFoundError(f"Failed to persist plot at {save_path}")
            
        logger.info(f"PLOT PERSISTED: {filename}")
    except Exception as e:
        logger.error(f"VISUALIZATION FAILURE: Could not save {name}. Error: {str(e)}")
        raise

# =============================================================================
# 3. ANALYTICAL & TEMPORAL HELPERS
# =============================================================================

def evaluate_model(actual: np.ndarray, forecast: np.ndarray) -> Dict[str, float]:
    """
    Calculates standard forecasting metrics: MAPE and RMSE.
    """
    mape = mean_absolute_percentage_error(actual, forecast)
    rmse = np.sqrt(mean_squared_error(actual, forecast))
    
    return {
        "MAPE": round(float(mape), 4),
        "RMSE": round(float(rmse), 4)
    }

def generate_time_dimensions(start_date: str, end_date: str) -> Tuple[List[datetime], List[str]]:
    """
    Generates a contiguous daily calendar and YYYYMM strings for database seeding.
    
    Input:
        start_date/end_date in 'YYYY-MM-DD' format.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    
    dates = [start + timedelta(days=i) for i in range(delta.days + 1)]
    yearmonths = [d.strftime("%Y%m") for d in dates]
    
    return dates, yearmonths

def compute_utilization_stats(series: np.ndarray) -> Dict[str, float]:
    """
    Provides a rapid descriptive statistical profile of a utilization series.
    """
    return {
        "mean_util": round(float(np.mean(series)), 2),
        "std_util": round(float(np.std(series)), 2),
        "max_util": round(float(np.max(series)), 2)
    }