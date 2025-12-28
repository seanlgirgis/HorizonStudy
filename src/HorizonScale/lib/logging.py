import logging
import sys
from horizonscale.lib.config import LOG_DIR

def init_root_logging(script_name: str):
    """
    Sets up a UNIFIED logging system for the entire process.
    - All modules write to the SAME file.
    - Automatically includes filename and method name in every line.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{script_name}.log"
    
    # Configure the Root Logger (the parent of ALL logs)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    
    # --- ENHANCEMENT: SILENCE EXTERNAL NOISE ---
    # This prevents Faker from flooding the log with 'Looking for locale' DEBUG messages
    logging.getLogger("faker").setLevel(logging.INFO)
    # -------------------------------------------
    
    # Wipe old handlers to ensure we don't double-log
    if root.hasHandlers():
        root.handlers.clear()

    # The Magic Formatter
    log_format = "%(asctime)s - [%(filename)s:%(funcName)s] - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # 1. Unified File Handler (mode='w' for fresh start)
    fh = logging.FileHandler(log_file, mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # 2. Unified Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    root.addHandler(fh)
    root.addHandler(ch)

    return root