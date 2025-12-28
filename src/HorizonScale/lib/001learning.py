# src/horizonscale/lib/scenario_generators.py  
# Author: Sean L Girgis  

import random
import numpy as np  
from horizonscale.lib.config import (
    Scenario  , PLOTS_DIR
)
from horizonscale.lib.logging import init_root_logging
from pathlib import Path

logger = init_root_logging(Path(__file__).stem)

from horizonscale.lib.config import GENERATOR_CONFIG


def generate_steady_growth(days: int, variant: str = 'normal', base_seed: int = 0) -> np.ndarray:  
    print("Steady Growth generator not yet implemented.")
    np.random.seed(base_seed)
    # Retrieve configuration for this specific scenario and variant
    cfg = GENERATOR_CONFIG[Scenario.STEADY_GROWTH][variant]
    print(f"Configuration for Steady Growth, variant '{variant}':type of cfg is '{type(cfg)}' and cfg = \n{cfg}")    
    
    # 1. Parameter Extraction
    base = np.random.uniform(*cfg["base_range"])
    print(f"type of base is: {type(base)} \n and base = {base}")
    # Total growth over the 3 years divided by total days gives daily drift
    mean_daily_growth = np.random.uniform(*cfg["growth_total_range"]) / days
    print(f"type of mean_daily_growth is: {type(mean_daily_growth)} \n and mean_daily_growth = {mean_daily_growth}")
    std_daily_growth = np.random.uniform(*cfg["std_growth"])
    noise_std = np.random.uniform(*cfg["noise_std"])
    season_amp = np.random.uniform(*cfg["season_amp"])        

    # 2. Mathematical Components
    # Trend: Cumulative sum of daily random growth steps
    daily_steps = np.random.normal(mean_daily_growth, std_daily_growth, days)  
    print(f"type of daily_steps is: {type(daily_steps)} \n and daily_steps = {daily_steps}")



    return np.zeros(days)



# Centralized Dispatcher
GENERATORS = {  
    Scenario.STEADY_GROWTH: generate_steady_growth  
#    ,Scenario.SEASONAL: generate_seasonal
#    ,Scenario.BURST: generate_burst
#    ,Scenario.LOW_IDLE: generate_low_idling
#    ,Scenario.CAPACITY_BREACH: generate_capacity_breach
#    ,Scenario.PLATEAU_DECLINE: generate_plateau_decline
}

def run_diagnostic_lab(scenario_enum: Scenario, test_days: int = 1095):
    """
    Experimental Lab: Generates, visualizes, and saves diagnostic plots 
    for a specific scenario's variants and stochastic independence.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from horizonscale.lib.config import TIME_START, PLOTS_DIR, SCENARIO_VARIANTS
    # 1. Setup Temporal Context & Seeds
    date_range = pd.date_range(start=TIME_START, periods=test_days, freq='D')
    #seed_alpha, seed_beta = 12345, 54321
    
    import random

    seed_alpha = random.randint(10000, 99999)

    # ensure seed_beta is different
    while True:
        seed_beta = random.randint(10000, 99999)
        if seed_beta != seed_alpha:
            break
    # 2. Extract Variants for this Scenario
    variants = SCENARIO_VARIANTS[scenario_enum]
    v_common = variants['common']
    v_rare = variants['rare']   
    
    # 3. Generate Data via Dispatcher
    gen_func = GENERATORS[scenario_enum] 
    print (type(gen_func)  )       
    print(f'''
          Generating data for scenario {scenario_enum} with variants '{v_common}' and '{v_rare}'...for n {test_days} days
          Using seeds: alpha={seed_alpha}, beta={seed_beta}
          ''')  
    series_common = gen_func(test_days, variant=v_common, base_seed=seed_alpha)
#    print(f"Generated series_common of length {len(series_common)}")
    
          

if __name__ == "__main__":
    # We can now test any (or all) scenarios with a single call
    test_scenarios = [
        Scenario.STEADY_GROWTH,
        # Scenario.SEASONAL, 
        # ... add more as you implement their math
    ]
    
    print(f"Starting Laboratory Diagnostics for {len(test_scenarios)} scenarios...")
    
    for scenario in test_scenarios:
        run_diagnostic_lab(scenario)
        
    print(f"\nSUCCESS: All diagnostic plots saved to {PLOTS_DIR}")
    