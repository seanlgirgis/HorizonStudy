# HorizonScale – Personal Portfolio Project

**Official Project Charter & Execution Plan**  
Goal: Build a 100% original, production-grade, resume-defining capacity planning & forecasting system from scratch, inspired by (but never copying) an existing internal tool you have access to.

**Project Owner:** Sean L Girgis  

**Target Completion:** 3–4 weeks (fast but extremely high quality)  
**Outcome:** Public GitHub repository + 12–18 page technical paper + killer resume bullets + interview-ready talking points

### Phase B – Simulation & Portfolio System Creation (Current Phase)

**Sub-task 1: Synthetic Data Generator – Scenario Simulation & Debugging**  
**Author:** Sean L Girgis  
**Description:** We have completed and locked two scenarios:  
- `001_scenario_Steady_Growth_simulation.ipynb` (moderate + breach variants with subtle seasonality/bursts).  
- `002_scenario_Seasonal_simulation.ipynb` (high/low amplitude with asymmetric peaks in Q4/summer, weekly business jumps, visible trend).  

These cover steady workloads and cyclic/seasonal patterns – essential for realistic infrastructure data.

**Remaining Scenarios to Fulfill**  
To achieve full diversity (1,000–5,000 hosts with varied behaviors), we need 4 more dedicated notebooks/functions for distinct patterns. This ensures compelling EDA, model comparison, and alerting demos in the portfolio.

Proposed Sequence (003–006):  
3. `003_scenario_Burst_simulation.ipynb`  
   - Dominant feature: Infrequent high spikes (e.g., batch jobs, deployments).  
   - Variants: Frequent bursts (high prob) vs. rare (low prob).  
   - Mild trend + noise.

4. `004_scenario_Low_Idling_simulation.ipynb`  
   - Dominant feature: Consistently low utilization (~10–20%) with minimal variation.  
   - Variants: Ultra-low idle vs. occasional small activity.  
   - For right-sizing recommendations.

5. `005_scenario_Capacity_Breach_simulation.ipynb`  
   - Dominant feature: Aggressive growth leading to sustained high util (>90%) and breaches.  
   - Variants: Fast breach vs. slow approach.  
   - Critical for alerting logic.

6. `006_scenario_Plateau_Decline_simulation.ipynb`  
   - Dominant feature: High start, gradual decline (decommissioning/migration).  
   - Variants: Sharp drop vs. slow plateau.  

**Why These Scenarios?**  
- Cover all common infrastructure patterns (growth, cycles, spikes, idle, breach, decline).  
- Enable strong portfolio visuals: Different models perform best on different patterns (e.g., Prophet on seasonal, SARIMA on trend).  
- Support business rules: Breach alerts, right-sizing for idle, anomaly detection for bursts.

**Next Steps:**  
- Proceed to `003_scenario_Burst_simulation.ipynb` (infrequent high spikes with baseline noise/trend).  
- Reply "proceed to burst" to start the new notebook.  

We can code all remaining scenarios – this will make HorizonScale incredibly impressive for SRE/Cloud interviews. Ready when you are! 🚀