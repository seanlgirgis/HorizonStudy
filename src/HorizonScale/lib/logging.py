"""
logging.py
Author: Sean L. Girgis

Purpose:
    Provides a unified, high-visibility logging framework for the HorizonScale 
    pipeline. It centralizes log formatting, destination management (Console + File), 
    and performance profiling tools.

Genesis (Entrance Criteria):
    - LOG_DIR must be defined in the project config.
    - Script name must be provided to ensure unique log file identification.

Success (Exit Criteria):
    - All telemetry and system events are captured with filename and function context.
    - External library noise (e.g., Faker) is suppressed to maintain log signal-to-noise ratio.
    - Execution durations are measured and reported for performance auditing.
"""

import logging
import sys
import time
from pathlib import Path
from contextlib import contextmanager
from horizonscale.lib.config import LOG_DIR

@contextmanager
def execution_timer(logger: logging.Logger, process_name: str = "Task"):
    """
    PROFILING UTILITY: Tracks and reports the real-world duration of a process block.
    Usage:
        with execution_timer(logger, "Master Parquet Generation"):
            ... logic ...
    """
    start_time = time.perf_counter()
    logger.info(f"STARTING: {process_name}...")
    try:
        yield
    finally:
        end_time = time.perf_counter()
        duration = end_time - start_time
        # Format duration into human-readable minutes and seconds
        minutes, seconds = divmod(duration, 60)
        logger.info(f"EXIT REPORT: {process_name} completed in {int(minutes)}m {seconds:.2f}s")

def init_root_logging(script_name: str) -> logging.Logger:
    """
    INITIALIZATION: Establishes the Root Logger configuration.
    
    This unified approach ensures:
    1. A single log file per script run in the /logs directory.
    2. Synchronized output to both the terminal (INFO level) and file (DEBUG level).
    3. Structural metadata (file:function) is injected into every message.
    """
    # Ensure the log directory exists before attempting to write
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{script_name}.log"
    
    # Access the Root Logger to control all downstream logging behavior
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    
    # --- NOISE REDUCTION LAYER ---
    # Suppress verbose 'DEBUG' logs from third-party libraries like Faker
    logging.getLogger("faker").setLevel(logging.INFO)
    # -----------------------------
    
    # IDEMPOTENCY: Clear existing handlers to prevent duplicate log entries 
    # if the initialization is called multiple times.
    if root.hasHandlers():
        root.handlers.clear()

    # STANDARD FORMAT: Time - [Source File:Source Function] - Level - Message
    log_format = "%(asctime)s - [%(filename)s:%(funcName)s] - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # 1. PERSISTENT LOG (File): Captures every detail for post-mortem analysis
    fh = logging.FileHandler(log_file, mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # 2. INTERACTIVE LOG (Stream): Provides high-level progress to the user
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    root.addHandler(fh)
    root.addHandler(ch)

    return root