docs/pipeline/10_risk_dashboard.md

### **File Overview: `10_risk_dashboard.py**`

The `10_risk_dashboard.py` script serves as the **Executive Presentation Layer** of the HorizonScale pipeline. It deploys an interactive, web-based dashboard using the Streamlit framework to visualize critical infrastructure risks. This interface allows analysts and stakeholders to drill down into the visual evidence and statistical metrics produced by the champion forecasting models.

---

### **1. UI Design & Layout**

The dashboard is engineered for high-density data review and executive decision-making.

* **Modernized Layout**: Utilizes a wide-mode configuration to maximize screen real estate for complex data tables and forecast imagery.
* **Executive Metrics (KPIs)**: Features high-level counters to track total risks detected, the number of high-priority (⭐) alerts, and the average projected peak utilization across the fleet.
* **Priority Highlighting**: Employs conditional formatting to apply a light pink background to rows flagged as volatile, guiding the user’s eye to the most critical infrastructure threats.

### **2. Data Acquisition & Interaction**

The application connects directly to the underlying analytical store to provide real-time insights.

* **Cached Database Connection**: Implements a read-only, cached connection to the DuckDB store to ensure rapid data retrieval and UI responsiveness.
* **Infrastructure Risk Audit Table**: Displays a sortable inventory of at-risk servers, including the specific resource type, the winning champion model, the earliest projected breach date, and the expected peak utilization.
* **Search and Filter**: Enables users to search for specific servers or resources through a dynamic selection interface.

### **3. Visual Evidence Drill-Down**

A core feature of the dashboard is the seamless integration between tabular risk data and the visual forecast gallery.

* **Dynamic Image Loading**: Allows users to select an at-risk server and instantly retrieve its corresponding forecast diagram from the `risk_visuals` directory.
* **Forecast Inspection**: Displays high-resolution PNGs showing the forecast trend, confidence intervals, and critical 95% thresholds.
* **Contextual Feedback**: Provides informative status messages identifying the specific model (Prophet or XGBoost) used to generate the displayed evidence.

### **4. Technical Implementation**

* **Container-Width Scaling**: Complies with modern web standards by dynamically scaling visual assets to fit the user's browser window.
* **Idempotency and Verification**: Includes error handling to alert the user if visual evidence is missing, ensuring the dashboard remains a reliable source of truth.
* **Performance Optimization**: Minimizes resource consumption by only loading visual assets upon explicit user request.

---

### **Deployment Command**

To launch the interactive dashboard, execute the following command from the project root:

```bash
streamlit run src/HorizonScale/pipeline/10_risk_dashboard.py

```

---

### **System Requirements**

* **Entrance Criteria**: A populated `capacity_risks` table in the database and a generated visual gallery in the `risk_visuals` directory.
* **Exit Criteria**: An active web interface accessible via a browser for infrastructure risk auditing.
* **Dependencies**: Relies on `streamlit` for the UI framework and `duckdb` for back-end data retrieval.