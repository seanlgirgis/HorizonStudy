# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation – High-Level Recap and Roadmap Ahead**  

**Author:** Sean L Girgis  
**Date:** December 21, 2025  

### Executive Summary  
At this milestone in HorizonScale, we've laid a rock-solid foundation for a cutting-edge infrastructure capacity planning tool. Inspired by enterprise-grade systems like Trenda, we've built an original system from scratch that simulates realistic server environments, processes vast amounts of time-series data, and delivers initial forecasts to guide business decisions on resource scaling. This positions HorizonScale as a showcase of strategic foresight in cloud and SRE operations—helping organizations avoid downtime, optimize costs, and plan ahead with confidence.  

We've completed the early sub-tasks of Phase B, focusing on data simulation and foundational analytics. Moving forward, we'll enhance the forecasting core, add intelligent recommendations, and wrap it in a user-friendly dashboard. This will culminate in a production-ready engine that not only predicts capacity needs over 6 months but highlights a critical 3-month window for actionable decisions, all while integrating modern AI-driven methodologies for superior accuracy and efficiency.  

### Recap of Work Completed (Bird's-Eye View)  
To date, we've transformed conceptual insights from Phase A into tangible, operational components. In business terms, this means we've created a simulated "digital twin" of a large-scale infrastructure environment—think thousands of servers with varying usage patterns like steady growth, seasonal spikes, or sudden bursts. This allows us to test and refine forecasting without relying on real-world data, ensuring privacy and scalability for portfolio demonstration.  

Key achievements:  
- **Synthetic Data Foundation**: Developed a generator that mimics real server metrics (CPU, memory, storage, network) across 2,500 hosts over three years (2023–2025). This creates believable scenarios, enabling reliable testing of capacity planning strategies.  
- **Data Ingestion and Preparation**: Built scripts to produce monthly CSV files for each resource type, then a pipeline to merge them into a unified dataset enriched with business hierarchies (e.g., app managers, owners). This streamlines data flow, reducing manual effort and errors in real-world applications.  
- **Exploratory Analysis**: Created a Jupyter notebook for initial data inspection, aggregation (e.g., weekly rollups), and preprocessing. This step ensures data quality, identifying trends like utilization patterns that inform business-level insights on resource efficiency.  
- **Baseline Forecasting**: Implemented a notebook using Prophet (a robust time-series tool from Meta) to produce 6-month forecasts with uncertainty bands. This provides a straightforward view of future capacity risks, evaluated against metrics like MAPE (targeting under 8% error for high reliability).  
- **Advanced Forecasting Prototype**: Advanced to a notebook with Darts and Temporal Fusion Transformer (TFT)—a neural network-based approach that captures complex patterns like seasonality and anomalies better than traditional methods. This elevates predictions from basic trends to nuanced, probabilistic insights, improving decision-making in volatile environments.  

In essence, we've built the "data backbone" and initial "prediction engine," demonstrating how businesses can forecast infrastructure needs to cut costs (e.g., avoiding over-provisioning) and mitigate risks (e.g., preventing breaches). All code is original, modular, and committed to GitHub with documentation, showcasing best practices in agile development.  

### Next Steps: Building the Full Forecasting Engine and Beyond  
With the data simulation and baseline models in place, we'll shift to enhancing the core engine, adding business intelligence features, and polishing for deployment. The focus remains on delivering value: empowering leaders to make informed, timely decisions on scaling resources while highlighting efficiency opportunities. We'll introduce hybrid methodologies blending statistical rigor with AI for even more accurate, explainable forecasts.  

Upcoming sub-tasks in Phase B (target: 7–10 days):  
1. **Expand Baseline Models**: Refine the baseline notebook to include SARIMA and ETS—proven statistical techniques for handling trends and seasonality. This provides a benchmark for reliability, ensuring forecasts align with business cycles like quarterly peaks.  
2. **Hybrid Advanced Models**: Create a new notebook integrating NeuralProphet (an evolution of Prophet with neural networks) and a LightGBM hybrid. This methodology combines deep learning for pattern recognition with gradient boosting for speed, yielding forecasts that adapt to dynamic workloads—ideal for enterprises facing unpredictable demand.  
3. **Core Forecasting Engine Script**: Develop `forecasting_engine.py`—a central script that automates 6-month projections with built-in uncertainty bands and a 3-month "decision horizon." In business terms, this flags immediate risks (e.g., potential capacity breaches) while offering long-term visibility, complete with logic for prioritizing actions like hardware upgrades.  
4. **Anomaly Detection and Recommendations**: Add `anomaly_recommender.py` to detect outliers (e.g., usage spikes) and suggest right-sizing (e.g., "Reallocate 20% memory from idle servers"). Using techniques like Isolation Forest, this turns raw forecasts into actionable advice, helping reduce waste and optimize budgets.  
5. **Interactive Dashboard**: Build `app.py` with Streamlit—a modern web framework for intuitive UIs. This will visualize forecasts, hierarchies, and recommendations in interactive charts, making it easy for stakeholders to explore "what-if" scenarios without deep technical knowledge.  
6. **Testing and Automation**: Introduce `tests/` folder with unit tests (using pytest) and set up GitHub Actions for CI. This ensures robustness, automating checks to maintain high-quality outputs as the system scales.  
7. **Containerization (Optional)**: Create a Dockerfile for easy deployment, allowing the tool to run anywhere—showcasing cloud-native readiness for enterprise adoption.  

These additions will complete Phase B, resulting in a fully functional system that not only predicts but prescribes solutions, bridging technical forecasting with business strategy.  

### Transition to Phases C and D  
Parallel to final Phase B work, we'll kick off Phase C: Crafting a 12–18 page technical paper detailing the business impact (e.g., cost savings from proactive scaling) and architecture. This includes diagrams, evaluation results, and future scaling ideas. We'll also produce a polished README with dashboard GIFs and a presentation deck for interviews.  

In Phase D, we'll distill this into resume bullets (e.g., "Engineered AI-hybrid forecasting tool reducing MAPE by 15% for 5,000-host simulations") and LinkedIn content, plus interview scripts to confidently discuss how HorizonScale drives operational excellence.  

This roadmap keeps us on track for a standout portfolio piece—ready to impress hiring managers in SRE, cloud, or infrastructure roles. Next immediate action: Expand baseline models in the notebook. Let's push forward! 🚀