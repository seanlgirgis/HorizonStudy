"""
scenario_generators.py
Author: Sean L. Girgis

Purpose:
    The "Mathematical DNA" library of the HorizonScale project. This module 
    contains the stochastic engines used to simulate realistic enterprise 
    utilization patterns (Steady Growth, Seasonal cycles, Bursts, etc.).

Genesis (Entrance Criteria):
    - Must be called with a valid 'variant' string matching config.py (UPPERCASE).
    - Requires NumPy for vector-based time-series synthesis.

Success (Exit Criteria):
    - Returns a NumPy ndarray representing daily p95 utilization.
    - All output values are strictly clipped between [0, 100].
    - Results are deterministic when provided with a consistent base_seed.
"""

import random
import numpy as np  
from pathlib import Path
from horizonscale.lib.config import (
    Scenario, PLOTS_DIR, GENERATOR_CONFIG, TIME_START, SCENARIO_VARIANTS
)
from horizonscale.lib.logging import init_root_logging

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

# =============================================================================
# 1. GENERATOR CORE: STEADY GROWTH
# =============================================================================
def generate_steady_growth(days: int, variant: str = 'NORMAL', base_seed: int = 0) -> np.ndarray:  
    """
    Simulates long-term linear drift combined with annual and weekly seasonality.
    Standardized to use direct dictionary lookups for performance.
    """
    np.random.seed(base_seed)
    cfg = GENERATOR_CONFIG[Scenario.STEADY_GROWTH][variant]
    
    # DNA Extraction
    base = np.random.uniform(*cfg["base_range"])
    mean_daily_growth = np.random.uniform(*cfg["growth_total_range"]) / days
    std_daily_growth = np.random.uniform(*cfg["std_growth"])
    noise_std = np.random.uniform(*cfg["noise_std"])
    season_amp = np.random.uniform(*cfg["season_amp"])
    
    t = np.arange(days)
    
    # Component Synthesis
    daily_steps = np.random.normal(mean_daily_growth, std_daily_growth, days)  
    trend = np.cumsum(daily_steps)  
    yearly_season = season_amp * np.cos(2 * np.pi * t / 365)  
    weekly_season = (season_amp / 4) * np.sin(2 * np.pi * t / 7)  
    noise = np.random.normal(0, noise_std, days)  
      
    util = base + trend + yearly_season + weekly_season + noise  
    return np.clip(util, 0, 100).astype(np.float32)

# =============================================================================
# 2. GENERATOR CORE: SEASONAL & BURST
# =============================================================================
def generate_seasonal(days: int, variant: str = 'BALANCED', base_seed: int = 0) -> np.ndarray:  
    """Simulates cyclical workloads (e.g., retail peaks) with amplified noise envelopes."""
    np.random.seed(base_seed)
    amp_multiplier = 2.0 if variant == 'EXTREME' else 1.0
    t = np.arange(days)
    
    base = 30 
    trend = np.cumsum(np.random.normal(0.008, 0.02, days))
    yearly_season = (15 * amp_multiplier) * np.cos(2 * np.pi * t / 365)
    weekly_season = 5 * np.sin(2 * np.pi * t / 7)
    
    noise_envelope = (np.cos(2 * np.pi * t / 365) + 1) / 2
    noise = np.random.normal(0, 2 + (3 * noise_envelope), days)
      
    util = base + trend + yearly_season + weekly_season + noise
    return np.clip(util, 0, 100).astype(np.float32)

def generate_burst(days: int, variant: str = 'MODERATE', base_seed: int = 0) -> np.ndarray:  
    """Simulates random compute spikes (e.g., batch jobs/backups) on a background trend."""
    np.random.seed(base_seed)
    t = np.arange(days)
    
    trend = np.cumsum(np.random.normal(0.005, 0.01, days))
    num_spikes = 35 if variant == 'EXTREME' else 15
    spike_indices = np.random.choice(t, size=num_spikes, replace=False)
    
    spikes = np.zeros(days)
    for idx in spike_indices:
        duration = np.random.randint(1, 4)
        magnitude = np.random.uniform(40, 75)
        spikes[idx : idx + duration] = magnitude
        
    util = 15 + trend + spikes + np.random.normal(0, 1.5, days)
    return np.clip(util, 0, 100).astype(np.float32)

# =============================================================================
# 3. GENERATOR CORE: IDLE & BREACH
# =============================================================================
def generate_low_idling(days: int, variant: str = 'STABLE', base_seed: int = 0) -> np.ndarray:  
    """Simulates underutilized legacy assets or standby nodes."""
    np.random.seed(base_seed)
    mean_val = 5 if variant == 'STABLE' else 10
    idle_floor = np.random.normal(mean_val, 0.5, days)
    return np.clip(idle_floor, 0, 100).astype(np.float32)

def generate_capacity_breach(days: int, variant: str = 'IMMINENT', base_seed: int = 0) -> np.ndarray:  
    """Simulates runaway exponential growth requiring urgent capacity intervention."""
    np.random.seed(base_seed)
    t = np.arange(days)
    growth_rate = 0.006 if variant == 'IMMINENT' else 0.009
    curve = 25 * np.exp(growth_rate * t / 10)
    
    util = curve + (5 * np.cos(2 * np.pi * t / 365)) + np.random.normal(0, 2.5, days)
    return np.clip(util, 0, 100).astype(np.float32)

# =============================================================================
# DISPATCHER & DIAGNOSTIC LAB
# =============================================================================
GENERATORS = {  
    Scenario.STEADY_GROWTH: generate_steady_growth,
    Scenario.SEASONAL: generate_seasonal,
    Scenario.BURST: generate_burst,
    Scenario.LOW_IDLE: generate_low_idling,
    Scenario.CAPACITY_BREACH: generate_capacity_breach
}

def run_diagnostic_lab(scenario_enum: Scenario, test_days: int = 1095):
    """
    VALIDATION SUITE: Generates comparative plots for Scenario Variants.
    Ensures mathematical logic matches visual expectations before full-scale generation.
    """
    import matplotlib.pyplot as plt
    import pandas as pd

    date_range = pd.date_range(start=TIME_START, periods=test_days, freq='D')
    seed = random.randint(10000, 99999)
    
    variants = SCENARIO_VARIANTS[scenario_enum]
    v_common = variants['common']
    v_rare = variants['rare']

    gen_func = GENERATORS[scenario_enum]
    series_common = gen_func(test_days, variant=v_common, base_seed=seed)
    series_rare = gen_func(test_days, variant=v_rare, base_seed=seed)

    plt.style.use('seaborn-v0_8-muted')
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(date_range, series_common, label=f"COMMON: {v_common}")
    ax.plot(date_range, series_rare, label=f"RARE: {v_rare}", color='firebrick', alpha=0.7)
    ax.set_title(f"Diagnostic: {scenario_enum.name} Behavioral Comparison")
    ax.set_ylabel("Utilization %")
    ax.legend()
    
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(PLOTS_DIR / f"diagnostic_{scenario_enum.name.lower()}.png")
    plt.close()
    logger.info(f"DIAGNOSTIC COMPLETE: {scenario_enum.name}")