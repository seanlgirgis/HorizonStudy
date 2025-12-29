# src/horizonscale/lib/scenario_generators.py  
# Author: Sean L Girgis  

import random
import numpy as np  
import matplotlib.pyplot as plt
import pandas as pd
from horizonscale.lib.config import (
    Scenario, PLOTS_DIR, GENERATOR_CONFIG, TIME_START, SCENARIO_VARIANTS
)
from horizonscale.lib.logging import init_root_logging
from pathlib import Path

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

def generate_steady_growth(days: int, variant: str = 'NORMAL', base_seed: int = 0) -> np.ndarray:  
    """
    MATHEMATICAL DNA: STEADY GROWTH
    Implements a random walk with drift, seasonality, and Gaussian noise.
    """
    np.random.seed(base_seed)
    
    # 1. Parameter Extraction
    # Ensure variant is lowercase to match the standardized config keys
    v_key = variant.upper() 
    cfg = GENERATOR_CONFIG[Scenario.STEADY_GROWTH][v_key]
    
    # Extract bounds from the config dictionary
    base = np.random.uniform(*cfg["base_range"])
    mean_daily_growth = np.random.uniform(*cfg["growth_total_range"]) / days
    std_daily_growth = np.random.uniform(*cfg["std_growth"])
    noise_std = np.random.uniform(*cfg["noise_std"])
    season_amp = np.random.uniform(*cfg["season_amp"])        

    # 2. Mathematical Component Construction
    # Trend: Cumulative sum of daily random growth steps (Drift)
    daily_steps = np.random.normal(mean_daily_growth, std_daily_growth, days)  
    trend = base + np.cumsum(daily_steps)

    # Seasonality: 7-day weekly cycle
    t = np.arange(days)
    seasonality = season_amp * np.sin(2 * np.pi * t / 7)

    # Noise: Unpredictable jitter
    noise = np.random.normal(0, noise_std, days)

    # 3. Final Assembly
    # Clip values to physically realistic bounds (0-100%)
    series = trend + seasonality + noise
    return np.clip(series, 0, 100).astype(np.float32)

# Centralized Dispatcher
# Maps Scenario Enum members to their respective math functions
GENERATORS = {  
    Scenario.STEADY_GROWTH: generate_steady_growth,
    # Placeholders for other scenarios until implemented
    Scenario.SEASONAL: lambda days, **kwargs: np.random.uniform(20, 40, days),
    Scenario.BURST: lambda days, **kwargs: np.random.uniform(10, 30, days),
    Scenario.LOW_IDLE: lambda days, **kwargs: np.random.uniform(1, 5, days),
    Scenario.CAPACITY_BREACH: lambda days, **kwargs: np.linspace(60, 100, days)
}

def run_diagnostic_lab(scenario_enum: Scenario, test_days: int = 1095):
    """
    Experimental Lab: Generates, visualizes, and saves diagnostic plots.
    """
    # 1. Setup Temporal Context & Independent Seeds
    date_range = pd.date_range(start=TIME_START, periods=test_days, freq='D')
    seed_alpha = random.randint(10000, 99999)
    
    # 2. Extract Variants for this Scenario
    variants = SCENARIO_VARIANTS[scenario_enum]
    v_common = variants['common']
    
    # 3. Generate Data via Dispatcher
    gen_func = GENERATORS[scenario_enum] 
    logger.info(f"Generating diagnostic: {scenario_enum.name} ({v_common})")
    
    series = gen_func(test_days, variant=v_common, base_seed=seed_alpha)
    
    # 4. Visualization
    plt.figure(figsize=(12, 5))
    plt.plot(date_range, series, label=f"Variant: {v_common}")
    plt.title(f"Laboratory Diagnostic: {scenario_enum.name} | Seed: {seed_alpha}")
    plt.ylabel("Utilization %")
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Ensure directory exists and save
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    plot_path = PLOTS_DIR / f"diagnostic_{scenario_enum.value}.png"
    plt.savefig(plot_path)
    plt.close()
    
    print(f"SUCCESS: Diagnostic plot saved to {plot_path}")

if __name__ == "__main__":
    # Test implemented scenarios
    test_scenarios = [Scenario.STEADY_GROWTH]
    
    print(f"Starting Laboratory Diagnostics for {len(test_scenarios)} scenarios...")
    for scenario in test_scenarios:
        run_diagnostic_lab(scenario)