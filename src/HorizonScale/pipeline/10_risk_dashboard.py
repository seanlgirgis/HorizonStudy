"""
10_risk_dashboard.py
Author: Sean L. Girgis

PURPOSE:
    The "Executive Presentation Layer" of the HorizonScale pipeline. This script 
    deploys an interactive Streamlit dashboard to visualize critical infrastructure 
    risks. It allows analysts to drill down into the evidence produced by the 
    Champion forecasting models.

UI DESIGN PHILOSOPHY:
    - Modernized Layout: Uses wide-mode configuration for high-density data review.
    - Priority Highlighting: Implements a subtle light pink (#FFF0F0) background for 
      rows flagged with the '⭐' priority symbol to guide the user's eye.
    - Dynamic Evidence Loading: Connects the tabular data directly to the PNG 
      visual gallery for seamless inspection.

FIX: 
    - Updated `st.image` parameter to `use_container_width=True` to comply with 
      modern Streamlit standards.
"""

import streamlit as st
import duckdb
from pathlib import Path

# Project-specific internal libraries
from horizonscale.lib.config import DB_PATH, MASTER_DATA_DIR

# Dashboard Global Configuration
st.set_page_config(page_title="HorizonScale Priority Risk Dashboard", layout="wide")
VISUALS_DIR = (MASTER_DATA_DIR / "risk_visuals").resolve()

@st.cache_resource
def get_db_connection():
    """
    Establishes a cached, read-only connection to the DuckDB analytics store.
    """
    return duckdb.connect(str(DB_PATH), read_only=True)

def run_dashboard():
    """
    ORCHESTRATION: Builds the UI components, fetches risk metrics, 
    and handles the interactive visual evidence drill-down.
    """
    st.title("🚨 Priority Infrastructure Risks")
    st.markdown("Focused capacity audit highlighting volatile or extreme utilization peaks.")
    
    con = get_db_connection()

    # 1. DATA ACQUISITION
    # Fetches prioritized risk statistics for the primary audit table.
    query = """
        SELECT 
            priority_flag as " ",
            host_id as "Server Name", 
            resource as "Resource", 
            winning_model as "Champion Model",
            earliest_breach_date as "Breach Date", 
            ROUND(projected_peak, 2) as "Peak %"
        FROM capacity_risks
        ORDER BY priority_flag DESC, projected_peak DESC
    """
    risk_df = con.execute(query).df()

    # 2. EXECUTIVE METRICS (KPIs)
    # Summarizes the state of the fleet using Streamlit metric components.
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Risks Detected", len(risk_df))
    c2.metric("High Priority (⭐)", len(risk_df[risk_df[' '] == '⭐']))
    c3.metric("Avg Fleet Peak", f"{round(risk_df['Peak %'].mean(), 1)}%")

    # 3. INFRASTRUCTURE RISK AUDIT TABLE
    # Applies conditional formatting to highlight volatile servers.
    st.subheader("Infrastructure Risk Audit")
    
    def highlight_priority(row):
        """Applies a light pink background to priority rows for visual emphasis."""
        return ['background-color: #FFF0F0' if row[' '] == '⭐' else '' for _ in row]

    st.dataframe(
        risk_df.style.apply(highlight_priority, axis=1),
        use_container_width=True
    )

    # 4. VISUAL EVIDENCE DRILL-DOWN
    # Allows the user to select an at-risk server and view the champion forecast graph.
    st.markdown("---")
    st.subheader("🔍 Visual Evidence Viewer")
    
    # Generate search labels combining ID and resource for selection clarity
    risk_df['search_label'] = risk_df[' '] + " " + risk_df['Server Name'] + " (" + risk_df['Resource'] + ")"
    
    selected_key = st.selectbox(
        "Select a server to inspect visual evidence:", 
        options=risk_df['search_label'].tolist()
    )
    
    # Trigger loading of the PNG forecast plot
    col_btn, _ = st.columns([1, 4])
    if col_btn.button("Load Forecast Diagram"):
        row = risk_df[risk_df['search_label'] == selected_key].iloc[0]
        h_id = row['Server Name']
        res = row['Resource']
        
        img_path = VISUALS_DIR / f"{h_id}_{res}.png"

        if img_path.exists():
            # Displaying the PNG gallery file with modern scaling
            st.image(str(img_path), use_container_width=True)
            st.info(f"Visualizing {row['Champion Model']} forecast for {h_id}. Shaded area shows confidence interval.")
        else:
            st.error(f"Visual evidence not found for {h_id}. Ensure Script 09 ran successfully.")

if __name__ == "__main__":
    run_dashboard()