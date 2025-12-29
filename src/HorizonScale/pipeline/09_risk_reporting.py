"""
09_risk_reporting.py
Author: Sean L. Girgis

MISSION: THE BALANCED ANALYST
    This script serves as the final analytical layer of the HorizonScale pipeline. 
    It identifies capacity breaches across the entire fleet and generates a 
    visual evidence gallery. It specifically highlights "Interesting" risks 
    characterized by high volatility or severe threshold violations.

ANALYTICAL LOGIC:
    - Risk Identification: Scans the Champion dataset for any yhat_upper >= 95%.
    - Priority Flagging (⭐): Denotes risks with high volatility (StdDev > 2) 
      or projected peaks exceeding 105%.
    - Visual Evidence: Automates the generation of 400+ forecast plots to 
      support executive capacity planning.

SUCCESS (EXIT CRITERIA):
    - Capacity risks table created with prioritized status flags.
    - Visual gallery populated in the 'risk_visuals' directory.
    - Risk distribution report logged, detailing model-specific breach counts.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Project-specific internal libraries
from horizonscale.lib.config import DB_PATH, MASTER_DATA_DIR
from horizonscale.lib.logging import init_root_logging, execution_timer

# Initialize specialized logging
logger = init_root_logging(Path(__file__).stem)

# Directory configurations for reporting
REPORT_DIR = (MASTER_DATA_DIR / "risk_visuals").resolve()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
RISK_REPORT_CSV = MASTER_DATA_DIR / "capacity_risk_report.csv"

def generate_all_risk_plots(con):
    """
    VISUAL GALLERY GENERATOR:
    Iterates through the identified risks and produces high-resolution PNGs 
    showing the forecast, confidence intervals, and breach thresholds.
    """
    # Pulling from the master risk inventory
    at_risk_servers = con.execute("SELECT host_id, resource FROM capacity_risks").fetchall()

    logger.info(f"Generating full visual gallery: {len(at_risk_servers)} plots...")
    logger.info(f"Target Directory: {REPORT_DIR}") 

    for host_id, resource in at_risk_servers:
        # Isolating the specific host/resource timeline
        plot_df = con.execute(f"""
            SELECT ds, yhat, yhat_lower, yhat_upper 
            FROM champion_view 
            WHERE host_id = '{host_id}' AND resource = '{resource}'
            AND ds >= '2025-01-01' AND ds <= '2026-04-01'
            ORDER BY ds ASC
        """).df()

        if plot_df.empty: 
            continue

        # Plot Configuration
        plt.figure(figsize=(10, 4))
        plt.plot(plot_df['ds'], plot_df['yhat'], label='Forecast', color='#1f77b4', linewidth=2)
        plt.fill_between(plot_df['ds'], plot_df['yhat_lower'], plot_df['yhat_upper'], 
                         color='#1f77b4', alpha=0.2, label='Confidence Interval')
        
        # Static Threshold for Breach Definition
        plt.axhline(y=95, color='red', linestyle='--', label='95% Threshold')
        
        plt.title(f"CAPACITY RISK: {host_id} | {resource.upper()}")
        plt.xlabel("Timeline (2025 - Early 2026)")
        plt.ylabel("Utilization %")
        plt.ylim(0, max(110, plot_df['yhat_upper'].max() + 5))
        plt.legend(loc='upper left')
        plt.grid(alpha=0.3)

        # Persistence to Disk
        plt.savefig(REPORT_DIR / f"{host_id}_{resource}.png")
        plt.close()

def run_risk_analysis():
    """
    MAIN ANALYTICS LOOP:
    Identifies breaches, calculates risk priority, and orchestrates plotting.
    """
    with execution_timer(logger, "Capacity Risk Analysis"):
        con = duckdb.connect(str(DB_PATH))
        
        # Create View for Champion Data
        champion_parquet = str(MASTER_DATA_DIR / "champion_forecast_master.parquet").replace("\\", "/")
        con.execute(f"CREATE OR REPLACE VIEW champion_view AS SELECT * FROM read_parquet('{champion_parquet}')")

        # RISK IDENTIFICATION & PRIORITIZATION
        # High volatility (>2) or high impact (>105%) triggers the priority flag (⭐).
        con.execute("""
            CREATE OR REPLACE TABLE capacity_risks AS
            WITH risk_stats AS (
                SELECT 
                    host_id, resource, winning_model,
                    MIN(ds) as earliest_breach_date,
                    MAX(yhat_upper) as projected_peak,
                    STDDEV(yhat) as forecast_volatility
                FROM champion_view
                WHERE yhat_upper >= 95 AND ds >= '2025-01-01'
                GROUP BY 1, 2, 3
            )
            SELECT *,
                CASE 
                    WHEN forecast_volatility > 2 OR projected_peak > 105 THEN '⭐' 
                    ELSE '' 
                END as priority_flag
            FROM risk_stats
            ORDER BY projected_peak DESC
        """)

        # MODEL-SPECIFIC DISTRIBUTION REPORTING
        summary = con.execute("""
            SELECT winning_model, priority_flag, COUNT(*) as count 
            FROM capacity_risks GROUP BY 1, 2 ORDER BY 1, 2 DESC
        """).df()
        
        print("\n--- RISK DISTRIBUTION BY MODEL ---")
        print(summary.to_string(index=False))

        # Final Visual Generation
        generate_all_risk_plots(con)
        
        con.close()
        logger.info(f"SUCCESS: Report data analyzed and visual gallery generated.")

if __name__ == "__main__":
    run_risk_analysis()