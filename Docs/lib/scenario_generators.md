docs/lib/scenario_generators.md

### **File Overview: `scenario_generators.py**`

The `scenario_generators.py` module acts as the **Mathematical DNA** library for the HorizonScale project. It contains a suite of stochastic engines built on NumPy that simulate high-fidelity enterprise utilization patterns. These generators ensure that the 2,000-server synthetic fleet exhibits realistic, varied, and deterministic behaviors for testing the forecasting pipeline.

---

### **1. Core Generation Engines**

The module features specialized functions to replicate specific server lifecycle behaviors:

* **Steady Growth**: Combines long-term linear drift with multi-layered seasonality (yearly and weekly cycles) and random noise to simulate standard enterprise application growth.
* **Seasonal Cycles**: Replicates cyclical workloads, such as retail peaks, using amplified noise envelopes and cyclical oscillation.
* **Burst Patterns**: Simulates random compute spikes—representative of batch jobs or backup processes—overlaid on a background utilization trend.
* **Low/Idle**: Models underutilized assets or standby nodes with a stable, low-utilization floor.
* **Capacity Breach**: Simulates runaway exponential growth curves designed to test the pipeline's ability to flag imminent resource exhaustion.

### **2. Technical Implementation**

* **Stochastic Determinism**: Every generator uses a `base_seed` to ensure that synthetic data is reproducible across different runs of the pipeline.
* **Vectorized Computation**: Utilizes NumPy `ndarrays` for efficient, high-performance time-series synthesis, supporting the rapid generation of 3-year histories for thousands of hosts.
* **Data Integrity**: All generated telemetry is strictly clipped to a `[0, 100]` range and cast to `float32` to optimize memory and storage footprint.

### **3. Dispatcher and Variants**

The module employs a centralized `GENERATORS` dictionary that maps standardized Enums to their respective functions. This allows the pipeline to dynamically call the appropriate engine based on the distribution weights defined in the configuration. It further supports "Common" and "Rare" variants for each scenario, ensuring a diverse risk profile within the synthetic dataset.

### **4. Diagnostic Validation**

The **`run_diagnostic_lab`** utility provides a visual validation suite.

* **Visual Auditing**: Generates comparative plots of different scenario variants.
* **Logical Verification**: Ensures that the mathematical output aligns with visual expectations (e.g., verifying an "Extreme" variant actually shows higher volatility) before the data is committed to the master database.

---

### **System Requirements**

* **Inputs**: Requires a valid variant string and a day count.
* **Outputs**: Returns a daily p95 utilization array.
* **Dependencies**: Relies on `numpy` for mathematical operations and the project's central configuration for behavioral parameters.