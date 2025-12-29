"""
config.py - Central Configuration Module
Standardized to lowercase values for project-wide consistency.
"""
"""
config.py - Central Configuration Module
Standardized to UPPERCASE values for Enum parity and SQL consistency.
"""
from pathlib import Path  
import enum  

# Project root calculation
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Database and Output Paths
DB_PATH = PROJECT_ROOT / "data" / "synthetic" / "horizonscale_synth.db"  
SQL_SCHEMA_DIR = PROJECT_ROOT / "data" / "synthetic" / "sql"  
PLOTS_DIR = PROJECT_ROOT / "plots"  
LOG_DIR = PROJECT_ROOT / "logs"  
LOG_LEVEL = "INFO"

# Time and Generation Defaults
TIME_START = "2023-01-01"  
TIME_END = "2025-12-01"  
MASTER_DATA_DIR = PROJECT_ROOT / "data" / "synthetic" / "master"  
MASTER_PARQUET_FILE = MASTER_DATA_DIR / "master_daily_2023_2025.parquet"  
DEFAULT_NUM_HOSTS = 2000  

# Legacy Export Paths
LEGACY_INPUT_DIR = PROJECT_ROOT / "data" / "legacy" / "inputs"
SYNTHETIC_DATA_DIR = LEGACY_INPUT_DIR 

# Reproducibility
SEED_MODULO = 2**32 - 1  

# --- Standardized UPPERCASE Scenario Enum ---
class Scenario(enum.Enum):  
    STEADY_GROWTH = 'STEADY_GROWTH'  
    SEASONAL = 'SEASONAL'  
    BURST = 'BURST'  
    LOW_IDLE = 'LOW_IDLE'  
    CAPACITY_BREACH = 'CAPACITY_BREACH'  

# Scenario Distribution
SCENARIO_DISTRIBUTION = {
    Scenario.STEADY_GROWTH: 0.35,
    Scenario.LOW_IDLE: 0.25,
    Scenario.SEASONAL: 0.20,
    Scenario.BURST: 0.15,
    Scenario.CAPACITY_BREACH: 0.05
}

# --- Standardized UPPERCASE Variants ---
SCENARIO_VARIANTS = {  
    Scenario.STEADY_GROWTH: {'common': 'NORMAL', 'rare': 'BREACH'},
    Scenario.SEASONAL: {'common': 'BALANCED', 'rare': 'EXTREME'},
    Scenario.BURST: {'common': 'MODERATE', 'rare': 'EXTREME'},
    Scenario.LOW_IDLE: {'common': 'STABLE', 'rare': 'VARIABLE'},
    Scenario.CAPACITY_BREACH: {'common': 'IMMINENT', 'rare': 'SEVERE'}
}  

VARIANT_WEIGHTS = {'common': 0.80, 'rare': 0.20}

# Generator Parameters (Updated keys to UPPERCASE)
GENERATOR_CONFIG = {
    Scenario.STEADY_GROWTH: {
        "NORMAL": {
            "base_range": (35, 55),
            "growth_total_range": (12, 18),
            "std_growth": (0.002, 0.006),
            "noise_std": (1.5, 2.5),
            "season_amp": (2.0, 4.0)
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

# Metadata remain lowercase as they are descriptive, not structural IDs
CLASSIFICATIONS = ["physical", "virtual"]  
DEPARTMENTS = ["Retail Banking", "Financial Services", "Small Business", "Corporate", 
               "Investment Banking", "Commercial Loans", "Wealth Management"]  
REGIONS = ["na", "emea", "latam", "asiapac"]  
SERVER_TYPES = ["unix", "linux", "windows", "other"]  

# Resource Types remains lowercase (standard for 'cpu', 'memory')
RESOURCE_TYPES = {  
    'cpu': {'capacity_field': 'max_cores', 'adjust_factor_range': (1.05, 1.25), 'noise_std_range': (2.0, 4.0)},  
    'memory': {'capacity_field': 'memory_gb', 'adjust_factor_range': (0.95, 1.10), 'noise_std_range': (3.0, 5.0)},  
    'disk': {'capacity_field': 'storage_capacity_mb', 'adjust_factor_range': (0.70, 0.90), 'noise_std_range': (5.0, 9.0)},  
    'network': {'capacity_field': None, 'adjust_factor_range': (0.85, 1.15), 'noise_std_range': (4.0, 7.0)}
}

# Legacy Export Metadata
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

# Forecast Settings
FORECAST_START_DATE = "2025-12-01" 
FORECAST_HORIZON = 180