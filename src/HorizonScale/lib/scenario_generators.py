# src/horizonscale/lib/scenario_generators.py  
# Author: Sean L Girgis  

import random
import numpy as np  
from pathlib import Path
from horizonscale.lib.config import (
    Scenario, PLOTS_DIR, GENERATOR_CONFIG, TIME_START, SCENARIO_VARIANTS
)
from horizonscale.lib.logging import init_root_logging

logger = init_root_logging(Path(__file__).stem)

def generate_steady_growth(days: int, variant: str = 'normal', base_seed: int = 0) -> np.ndarray:  
    """Steady growth with cumulative drift, seasonality (Peaking Dec/Jan), and unique seeding."""
    np.random.seed(base_seed)
    cfg = GENERATOR_CONFIG[Scenario.STEADY_GROWTH][variant]
    
    # 1. Parameter Extraction
    base = np.random.uniform(*cfg["base_range"])
    mean_daily_growth = np.random.uniform(*cfg["growth_total_range"]) / days
    std_daily_growth = np.random.uniform(*cfg["std_growth"])
    noise_std = np.random.uniform(*cfg["noise_std"])
    season_amp = np.random.uniform(*cfg["season_amp"])
    
    t = np.arange(days)
    
    # 2. Mathematical Components
    daily_steps = np.random.normal(mean_daily_growth, std_daily_growth, days)  
    trend = np.cumsum(daily_steps)  
      
    # Use Cosine to peak at Jan 1st and Dec 31st
    yearly_season = season_amp * np.cos(2 * np.pi * t / 365)  
    weekly_season = (season_amp / 4) * np.sin(2 * np.pi * t / 7)  
      
    noise = np.random.normal(0, noise_std, days)  
      
    # 3. Final Aggregation
    util = base + trend + yearly_season + weekly_season + noise  
    return np.clip(util, 0, 100)


def generate_seasonal(days: int, variant: str = 'balanced', base_seed: int = 0) -> np.ndarray:  
    """Periodic cycles with an added slight upward trend to simulate a growing seasonal business."""
    np.random.seed(base_seed)
    
    amp_multiplier = 2.0 if variant == 'extreme' else 1.0
    t = np.arange(days)
    
    # 1. Base and Trend (Slight steady growth enhancement)
    base = 30 
    mean_daily_growth = 0.008 
    std_daily_growth = 0.02
    daily_steps = np.random.normal(mean_daily_growth, std_daily_growth, days)
    trend = np.cumsum(daily_steps)

    # 2. Mathematical Components (Cosine for Dec/Jan peaks)
    yearly_season = (15 * amp_multiplier) * np.cos(2 * np.pi * t / 365)
    weekly_season = 5 * np.sin(2 * np.pi * t / 7)
    
    # 3. Dynamic Noise: Higher noise during peak seasons
    noise_envelope = (np.cos(2 * np.pi * t / 365) + 1) / 2
    noise = np.random.normal(0, 2 + (3 * noise_envelope), days)
      
    util = base + trend + yearly_season + weekly_season + noise
    return np.clip(util, 0, 100)


def generate_burst(days: int, variant: str = 'moderate', base_seed: int = 0) -> np.ndarray:  
    """Low base with random spikes, layered with non-deterministic global trends."""
    np.random.seed(base_seed)
    t = np.arange(days)
    
    # --- Layer 1: Stochastic Background (Unique per Host) ---
    # Randomizing the parameters themselves makes the trend un-reproducible
    rand_mean = np.random.uniform(0.003, 0.007) 
    rand_std = np.random.uniform(0.008, 0.015)
    trend = np.cumsum(np.random.normal(rand_mean, rand_std, days))
    
    # Randomized seasonality amplitudes
    rand_yr_amp = np.random.uniform(2.0, 5.0)
    rand_wk_amp = np.random.uniform(1.0, 3.0)
    yearly_season = rand_yr_amp * np.cos(2 * np.pi * t / 365)
    weekly_season = rand_wk_amp * np.sin(2 * np.pi * t / 7)
    
    # --- Layer 2: Primary Behavior (The Bursts) ---
    num_spikes = 35 if variant == 'extreme' else 15
    spike_indices = np.random.choice(t, size=num_spikes, replace=False)
    
    spikes = np.zeros(days)
    for idx in spike_indices:
        duration = np.random.randint(1, 4)
        magnitude = np.random.uniform(40, 75)
        spikes[idx : idx + duration] = magnitude
        
    noise = np.random.normal(0, 1.5, days)
    
    util = 15 + trend + yearly_season + weekly_season + spikes + noise
    return np.clip(util, 0, 100)


def generate_low_idling(days: int, variant: str = 'stable', base_seed: int = 0) -> np.ndarray:  
    """Consistently low utilization with unique, randomized environmental drift."""
    np.random.seed(base_seed)
    t = np.arange(days)
    
    # --- Layer 1: Stochastic Background ---
    rand_mean = np.random.uniform(0.001, 0.003)
    trend = np.cumsum(np.random.normal(rand_mean, 0.005, days))
    rand_yr_amp = np.random.uniform(0.5, 2.0)
    yearly_season = rand_yr_amp * np.cos(2 * np.pi * t / 365)
    
    # --- Layer 2: Primary Behavior ---
    mean_val = 5 if variant == 'stable' else 10
    noise_level = 0.4 if variant == 'stable' else 1.2
    idle_floor = np.random.normal(mean_val, noise_level, days)
    
    util = idle_floor + trend + yearly_season
    return np.clip(util, 0, 100)


def generate_capacity_breach(days: int, variant: str = 'imminent', base_seed: int = 0) -> np.ndarray:  
    """Exponential growth with randomized seasonal pressure points."""
    np.random.seed(base_seed)
    t = np.arange(days)
    
    # --- Layer 1: Stochastic Background ---
    rand_yr_amp = np.random.uniform(5.0, 10.0)
    yearly_season = rand_yr_amp * np.cos(2 * np.pi * t / 365)
    weekly_season = np.random.uniform(1.0, 4.0) * np.sin(2 * np.pi * t / 7)
    
    # --- Layer 2: Primary Behavior ---
    growth_rate = 0.006 if variant == 'imminent' else 0.009
    base = np.random.uniform(20, 30)
    curve = base * np.exp(growth_rate * t / 10)
    
    util = curve + yearly_season + weekly_season + np.random.normal(0, 2.5, days)
    return np.clip(util, 0, 100)

# def generate_plateau_decline(days: int, variant: str = 'sharp', base_seed: int = 0) -> np.ndarray:  
#     """Starts high, maintains a plateau, then drops off."""
#     np.random.seed(base_seed)
#     t = np.arange(days)
    
#     # Segment lengths
#     plateau_end = int(days * 0.6)
    
#     util = np.zeros(days)
#     util[:plateau_end] = 70 + np.random.normal(0, 2, plateau_end)
    
#     # Decline segment
#     decline_len = days - plateau_end
#     drop_rate = 0.5 if variant == 'sharp' else 0.1
#     decline = 70 - (np.arange(decline_len) * drop_rate)
    
#     util[plateau_end:] = decline + np.random.normal(0, 2, decline_len)
    
#     return np.clip(util, 0, 100)


# Centralized Dispatcher
GENERATORS = {  
    Scenario.STEADY_GROWTH: generate_steady_growth
    ,Scenario.SEASONAL: generate_seasonal  
    ,Scenario.BURST: generate_burst 
    ,Scenario.LOW_IDLE: generate_low_idling 
    ,Scenario.CAPACITY_BREACH: generate_capacity_breach
#    ,Scenario.PLATEAU_DECLINE: generate_plateau_decline
}

def run_diagnostic_lab(scenario_enum: Scenario, test_days: int = 1095):
    """
    Experimental Lab: Generates, visualizes, and saves diagnostic plots.
    """
    import matplotlib.pyplot as plt
    import pandas as pd

    date_range = pd.date_range(start=TIME_START, periods=test_days, freq='D')
    
    seed_alpha = random.randint(10000, 99999)
    while True:
        seed_beta = random.randint(10000, 99999)
        if seed_beta != seed_alpha:
            break

    variants = SCENARIO_VARIANTS[scenario_enum]
    v_common = variants['common']
    v_rare = variants['rare']

    gen_func = GENERATORS[scenario_enum]
    series_common = gen_func(test_days, variant=v_common, base_seed=seed_alpha)
    series_rare = gen_func(test_days, variant=v_rare, base_seed=seed_alpha)
    series_random_b = gen_func(test_days, variant=v_common, base_seed=seed_beta)

    plt.style.use('seaborn-v0_8-muted')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    ax1.plot(date_range, series_common, label=f"{v_common}", alpha=0.8)
    ax1.plot(date_range, series_rare, label=f"{v_rare}", color='firebrick', alpha=0.7)
    ax1.set_title(f"Variant Comparison: {scenario_enum.name} (Seed: {seed_alpha})", fontsize=14)
    ax1.set_ylabel("Utilization %")
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.5)

    ax2.plot(date_range, series_common, label=f"Host A (Seed: {seed_alpha})", alpha=0.8)
    ax2.plot(date_range, series_random_b, label=f"Host B (Seed: {seed_beta})", color='forestgreen', alpha=0.7)
    ax2.set_title(f"Stochastic Independence: {scenario_enum.name} Randomness Verification", fontsize=14)
    ax2.set_ylabel("Utilization %")
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle='--', alpha=0.5)

    plt.tight_layout()
    
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"diagnostic_{scenario_enum.name.lower()}_verification.png"
    plt.savefig(PLOTS_DIR / filename, dpi=300)
    plt.close()
    
    logger.info(f"Verified {scenario_enum.name} -> Saved: {filename}")

if __name__ == "__main__":
    test_scenarios = list(GENERATORS.keys())
    
    print(f"Starting Laboratory Diagnostics for {len(test_scenarios)} scenarios...")
    for scenario in test_scenarios:
        run_diagnostic_lab(scenario)
        
    print(f"\nSUCCESS: All diagnostic plots saved to {PLOTS_DIR}")