"""
config.py
Author: Sean L. Girgis

Purpose:
    Centralized configuration engine for the HorizonScale synthetic pipeline.
    It defines the environmental paths, behavioral distributions (DNA), 
    and forecasting parameters used across all 00-04 pipeline stages.

Genesis (Entrance Criteria):
    - Python environment must support pathlib and enum.
    - Project folder structure should ideally follow the defined ROOT hierarchy.

Success (Exit Criteria):
    - Consistent UPPERCASE parity for structural Enums (Scenario/Variants).
    - Accurate path mapping for DuckDB, Parquet, and Legacy CSV exports.
    - Reproducible seed modulos and distribution weights for synthetic generation.
"""

from pathlib import Path  
import enum  

# =============================================================================
# 1. DIRECTORY & PATH ARCHITECTURE
# =============================================================================
# Project root calculation: Navigates from src/horizonscale/lib/ to project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Core Database and Metadata Paths
DB_PATH = PROJECT_ROOT / "data" / "synthetic" / "horizonscale_synth.db"  
SQL_SCHEMA_DIR = PROJECT_ROOT / "data" / "synthetic" / "sql"  
PLOTS_DIR = PROJECT_ROOT / "plots"  
LOG_DIR = PROJECT_ROOT / "logs"  
LOG_LEVEL = "INFO"

# Telemetry Persistence (Master Parquet)
MASTER_DATA_DIR = PROJECT_ROOT / "data" / "synthetic" / "master"  
MASTER_PARQUET_FILE = MASTER_DATA_DIR / "master_daily_2023_2025.parquet"  

# Legacy Simulation Paths (Monthly CSVs)
LEGACY_INPUT_DIR = PROJECT_ROOT / "data" / "legacy" / "inputs"
SYNTHETIC_DATA_DIR = LEGACY_INPUT_DIR 

# =============================================================================
# 2. GENERATION PARAMETERS
# =============================================================================
TIME_START = "2023-01-01"  
TIME_END = "2025-12-01"  
DEFAULT_NUM_HOSTS = 2000  

# Reproducibility Seed
SEED_MODULO = 2**32 - 1  

# --- Standardized UPPERCASE Scenario Enum ---
# Used for SQL parity and internal generator dispatching
class Scenario(enum.Enum):  
    STEADY_GROWTH = 'STEADY_GROWTH'  
    SEASONAL = 'SEASONAL'  
    BURST = 'BURST'  
    LOW_IDLE = 'LOW_IDLE'  
    CAPACITY_BREACH = 'CAPACITY_BREACH'  

# Enterprise Behavioral Distribution (The "Market Share" of server types)
SCENARIO_DISTRIBUTION = {
    Scenario.STEADY_GROWTH: 0.35,
    Scenario.LOW_IDLE: 0.25,
    Scenario.SEASONAL: 0.20,
    Scenario.BURST: 0.15,
    Scenario.CAPACITY_BREACH: 0.05
}

# --- Standardized UPPERCASE Variants ---
# Defines the Risk Profile for each Scenario
SCENARIO_VARIANTS = {  
    Scenario.STEADY_GROWTH: {'common': 'NORMAL', 'rare': 'BREACH'},
    Scenario.SEASONAL: {'common': 'BALANCED', 'rare': 'EXTREME'},
    Scenario.BURST: {'common': 'MODERATE', 'rare': 'EXTREME'},
    Scenario.LOW_IDLE: {'common': 'STABLE', 'rare': 'VARIABLE'},
    Scenario.CAPACITY_BREACH: {'common': 'IMMINENT', 'rare': 'SEVERE'}
}  

VARIANT_WEIGHTS = {'common': 0.80, 'rare': 0.20}

# =============================================================================
# 3. MATHEMATICAL DNA (Generator Configuration)
# =============================================================================
GENERATOR_CONFIG = {
    Scenario.STEADY_GROWTH: {
        "NORMAL": {
            "base_range": (35, 55),          # Initial utilization %
            "growth_total_range": (12, 18),  # Total growth over 3 years
            "std_growth": (0.002, 0.006),    # Volatility of daily drift
            "noise_std": (1.5, 2.5),         # Random jitter
            "season_amp": (2.0, 4.0)         # Weekly cycle magnitude
        },
        "BREACH": {
            "base_range": (70, 80),
            "growth_total_range": (18, 22),
            "std_growth": (0.003, 0.007),
            "noise_std": (1.0, 2.0),
            "season_amp": (3.0, 5.0)
        }
    }
}

# =============================================================================
# 4. METADATA & RESOURCE MAPPING
# =============================================================================
# Metadata remains lowercase (Descriptive, not structural)
CLASSIFICATIONS = ["physical", "virtual"]  
DEPARTMENTS = ["Retail Banking", "Financial Services", "Small Business", "Corporate", 
               "Investment Banking", "Commercial Loans", "Wealth Management"]  
REGIONS = ["na", "emea", "latam", "asiapac"]  
SERVER_TYPES = ["unix", "linux", "windows", "other"]  

# Resource specific noise and adjustment factors
RESOURCE_TYPES = {  
    'cpu': {'capacity_field': 'max_cores', 'adjust_factor_range': (1.05, 1.25), 'noise_std_range': (2.0, 4.0)},  
    'memory': {'capacity_field': 'memory_gb', 'adjust_factor_range': (0.95, 1.10), 'noise_std_range': (3.0, 5.0)},  
    'disk': {'capacity_field': 'storage_capacity_mb', 'adjust_factor_range': (0.70, 0.90), 'noise_std_range': (5.0, 9.0)},  
    'network': {'capacity_field': None, 'adjust_factor_range': (0.85, 1.15), 'noise_std_range': (4.0, 7.0)}
}

# Legacy Export Metadata (For 02_export_monthly_csvs.py)
RESOURCE_FILE_PREFIXES = {
    'cpu': 'cpu_utilization',
    'memory': 'memory_stats',
    'disk': 'storage_usage',
    'network': 'net_throughput'
}

CAPACITY_FIELDS = {
    'cpu': 'max_cores',
    'memory': 'memory_gb',
    'disk': 'storage_mb',
    'network': 'throughput_mbps'
}

# =============================================================================
# 5. FORECASTING PARAMETERS
# =============================================================================
FORECAST_START_DATE = "2025-12-01" 
FORECAST_HORIZON = 180  # 6-month projection