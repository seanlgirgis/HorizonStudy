# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 2: CSV File Generation – Scenario Design & Realism**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

### Ensuring Generated Data Supports Strong ML Scenarios

To guarantee the synthetic data drives meaningful forecasting results, we must design the generation process to include diverse, realistic scenarios that highlight the strengths of our models (e.g., Prophet, SARIMA, NeuralProphet, TFT hybrids). This will produce clear signals for trends, seasonality, anomalies, and capacity risks — allowing us to demonstrate accurate predictions, uncertainty bands, and actionable insights in the dashboard.

#### Key Scenarios to Embed in Data Generation
We will create controlled variations in utilization patterns across hosts and time periods. This ensures the ML pipeline can learn and forecast effectively.

1. **Steady Growth**  
   - Gradual upward trend (e.g., 1–5% monthly increase).  
   - Use case: Models detect long-term capacity needs.  
   - Example: Servers in "Retail Banking" during peak seasons.

2. **Seasonal Patterns**  
   - Cyclic fluctuations (e.g., weekly peaks Mon–Fri, yearly holiday spikes in December).  
   - Use case: Prophet/TFT excel at seasonality decomposition.  
   - Example: Higher util on "Financial Services" hosts during quarter-end.

3. **Sudden Bursts/Anomalies**  
   - Short spikes (e.g., 2–3 days of 90–100% util due to events).  
   - Use case: Anomaly detection and uncertainty bands.  
   - Example: "Investment Banking" hosts during market volatility.

4. **Low/Idle Utilization**  
   - Consistently <10% util (exclusion candidates).  
   - Use case: Preprocessing filters and right-sizing recommendations.

5. **Capacity Breaches**  
   - Forecasted util exceeds max_cores/memory_gb within 3–6 months.  
   - Use case: Decision horizon alerts and risk flagging.

6. **Plateaus or Declines**  
   - Stable or decreasing util (e.g., decommissioned servers).  
   - Use case: Models show no immediate action needed.

#### Implementation Strategy
- **Per-Host Scenarios**: Assign each host a scenario type (e.g., 20% steady growth, 20% seasonal, etc.) via a new DB column or config.  
- **Time-Series Generation**: For each host, generate daily P95 util based on its scenario:  
  - Baseline: Linear trend + noise.  
  - Seasonal: Add sine waves (weekly/yearly).  
  - Burst: Inject spikes (e.g., +50% for 2 days).  
  - Use Polars for efficient vectorized ops.  
- **Realism Controls**:  
  - Cap util at 100%.  
  - Correlate with classification/server_type (e.g., Windows VMs more bursty).  
  - Vary by region/department (e.g., NA retail = holiday spikes).  
- **Validation**: Post-generation, run quick checks (e.g., count breaches, plot samples) to ensure balanced scenarios.

#### Example Generation Logic (Snippet)
```python
# In CSV exporter
def generate_util_series(host, scenario):
    base_util = 50.0  # Starting point
    if scenario == "growth":
        base_util += np.linspace(0, 20, len(dates))  # +20% over time
    elif scenario == "seasonal":
        base_util += 20 * np.sin(2 * np.pi * np.arange(len(dates)) / 365)
    # Add noise, cap at 100, add bursts if needed
    return np.clip(base_util + np.random.normal(0, 5, len(dates)), 0, 100)
```

This ensures our ML models will shine: accurate forecasts, anomaly flags, and clear decision windows.  

Ready to build the first CSV exporter with these scenarios? Let me know. 🚀