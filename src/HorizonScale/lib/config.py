"""
# HorizonScale Configuration Module  
# Author: Sean L Girgis  
#
# Purpose of the script:
#     This module serves as the central configuration hub for the entire HorizonScale project. 
#     It defines all constants, paths, enumerations, and settings used across the synthetic data 
#     generation pipeline, data processing, exploratory data analysis (EDA), baseline and advanced 
#     forecasting models, anomaly detection, recommendations, and dashboard visualization. 
#     By centralizing these elements, the module ensures consistency, ease of maintenance, and 
#     modularity throughout the system. It handles project-wide paths (e.g., database, logs, plots), 
#     default parameters for data generation (e.g., number of hosts, time ranges), resource-specific 
#     configurations, scenario variants for synthetic data, and constants tailored for EDA, baselines, 
#     and model comparisons. This file is imported in nearly every script and notebook in the project 
#     to access these shared settings without duplication.
#
# What it does:
#     - Computes the project root directory dynamically based on the location of this file.
#     - Defines paths for the DuckDB database, SQL schemas, output directories (plots, logs), 
#       master/processed Parquet files, and legacy CSV directories.
#     - Sets logging level and other global constants like resource file prefixes.
#     - Provides lists for categorical metadata such as regions, server types, classifications, 
#       and business departments used in synthetic host generation.
#     - Establishes time ranges for data generation (start/end dates) and default parameters 
#       like number of hosts and years.
#     - Configures resource types with capacity fields, adjustment factors, and noise levels 
#       for realistic synthetic utilization simulation.
#     - Defines an Enum for Scenario types used in data generation (e.g., STEADY_GROWTH, SEASONAL).
#     - Maps scenario variants (common/rare) to control diversity in generated patterns.
#     - Sets a seed modulo for reproducible randomness.
#     - Maps resource-specific capacity fields from the hosts table.
#     - Specifies EDA constants for raw and processed data, including required columns, 
#       sample sizes for plots, plot titles, and stats directories.
#     - Defines baseline forecasting defaults like sample host IDs, default host/resource, 
#       forecast start date, and horizon (180 days).
#     - Provides comparison constants for model types (Prophet, SARIMA, ETS) and plot titles.
#     Overall, this module acts as a single source of truth for all configurable elements, 
#     allowing easy tweaks (e.g., changing years or hosts) without modifying core logic elsewhere.
#
# Dependencies:
#     - pathlib: Used for cross-platform path manipulation and dynamic project root calculation.
#     - enum: Imported to define the Scenario Enum for typed scenario references in data generation.
#     No external packages beyond Python standard library are required here, but the constants 
#     defined are used in conjunction with libraries like Polars, DuckDB, Pandas, Matplotlib, 
#     Seaborn, Prophet, Statsmodels, Darts, LightGBM, Streamlit, etc., in other modules.
#
# Outputs or Results:
#     - This is a pure configuration module, so it doesn't produce files, databases, or runtime 
#       outputs directly. Instead, it exports a suite of variables, constants, dictionaries, lists, 
#       and an Enum that are imported and used by other scripts.
#     - Key "outputs" include:
#       - PROJECT_ROOT: Path object for the project base directory.
#       - DB_PATH: Path to the DuckDB database file.
#       - Various directories (PLOTS_DIR, LOG_DIR, MASTER_DATA_DIR, etc.) as Path objects.
#       - Lists like REGIONS, SERVER_TYPES, DEPARTMENTS for metadata generation.
#       - Dictionaries like RESOURCE_TYPES, SCENARIO_VARIANTS, CAPACITY_FIELDS for data sim and processing.
#       - Enum: Scenario for typed scenario selection.
#       - EDA and baseline constants (e.g., EDA_REQUIRED_COLS, MODEL_TYPES) for analysis and modeling.
#     - Results: Enables consistent configuration across the pipeline; changes here propagate 
#       to all dependent scripts, affecting data generation (e.g., years/hosts), paths (e.g., outputs), 
#       and behaviors (e.g., forecast horizon).
#
# What the code inside is doing:
#     The code is structured as a series of assignments and definitions, grouped logically by category:
#     - First, import necessary modules (Path from pathlib, enum).
#     - Compute PROJECT_ROOT by navigating up three levels from this file's location (__file__).
#     - Database settings: Set DB_PATH and SQL_SCHEMA_DIR relative to PROJECT_ROOT.
#     - Output directories: Define PLOTS_DIR, LOG_DIR, and set LOG_LEVEL as a string ('INFO').
#     - Master Parquet Paths: Create MASTER_DATA_DIR and MASTER_PARQUET_FILE for raw synthetic data.
#     - Processed Parquet Paths: Similarly for PROCESSED_DATA_DIR and PROCESSED_PARQUET_FILE.
#     - Legacy CSV Paths: Define LEGACY_INPUT_DIR and SYNTHETIC_DATA_DIR (aliased to the same).
#     - Resource file prefixes: Dict mapping resources ('cpu', etc.) to filename prefixes.
#     - Regions list: Hardcoded list of geographic regions for host metadata.
#     - Server types: List of OS types.
#     - Classifications: Physical/virtual server types.
#     - Business units / departments: List of sample departments for hierarchy.
#     - Time range: Strings for TIME_START and TIME_END.
#     - Default generation params: DEFAULT_NUM_HOSTS (2000), DEFAULT_YEARS (range 2023-2026).
#     - Resource types: Dict with sub-dicts for each resource, including capacity_field, adjust_factor, noise_std.
#     - Scenario Enum: Defines class Scenario(enum.Enum) with values like STEADY_GROWTH=1, etc.
#     - Scenario variants: Dict mapping each Scenario to a sub-dict of 'common' and 'rare' variant strings.
#     - SEED_MODULO: Large integer (2**32 - 1) for seeding consistency.
#     - Capacity fields: Dict remapping resources to their capacity columns in hosts table.
#     - EDA-Specific Constants (Raw): Lists/ dicts for required columns, sample hosts, plot titles, stats dir.
#     - Processed EDA Constants: Similar but for processed data with rolling features.
#     - Baseline Defaults: SAMPLE_HOST_IDS list, DEFAULT_HOST_ID, DEFAULT_RESOURCE, FORECAST_START_DATE, FORECAST_HORIZON.
#     - Comparison Constants: MODEL_TYPES list, COMPARISON_PLOT_TITLES dict.
#     The code is declarative and runs at import time, making all these available as module-level globals.
#     No functions or logic beyond path calculations and assignments; it's designed for import and use.
"""


from pathlib import Path  
import enum  

# Project root (four levels up from src/horizonscale/lib/config.py)  
# 0: lib, 1: horizonscale, 2: src, 3: HorizonStudy
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Database settings  
# Now these directories will be created in C:\pyproj\HorizonStudy\
DB_PATH = PROJECT_ROOT / "data" / "synthetic" / "horizonscale_synth.db"  
SQL_SCHEMA_DIR = PROJECT_ROOT / "data" / "synthetic" / "sql"  
PLOTS_DIR = PROJECT_ROOT / "plots"  
LOG_DIR = PROJECT_ROOT / "logs"  
LOG_LEVEL = "INFO"