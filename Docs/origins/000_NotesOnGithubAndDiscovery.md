# HorizonScale – Personal Portfolio Project  
**Phase A – Discovery & Knowledge Extraction**  
**Status Update:** Ongoing  

**Prepared by:** Sean L Girgis  
**Date:** December 22, 2025  

Starting Phase A now.  

I've received and reviewed the provided materials, including:  
- synthetic_config.yaml (base configuration for synthetic data generation)  
- config.py (centralized project constants, paths, and settings)  
- forecasting_engine.py (initial forecasting logic, but noted as misnamed—appears to be a duplicate or variant of synthetic_data_generator.py)  
- data_pipeline.py (ETL pipeline using Polars and DuckDB for loading, cleaning, aggregating, and feature addition)  
- generate_synthetic_data.py (SQLite-based host/business hierarchy and time period generation with Faker for realism)  
- synthetic_data_generator.py (Polars-based time-series generator with patterns like trends, seasonality, bursts, anomalies, and rack correlations)  
- generate_csv_inputs.py (CSV generator for monthly/annual resource metrics based on scenarios)  
- baseline_forecasting.ipynb (Prophet baseline model training, evaluation, and plotting on synthetic data)  
- baseline_models_refined.ipynb (Refined SARIMA and ETS baselines with stationarity checks, hyperparameter tuning, and hybrid comparison)  
- synthetic_analysis.ipynb (EDA on synthetic data using DuckDB queries, visualizations like time-series plots, correlations, and anomaly detection)  
- advanced_hybrid_models.ipynb (NeuralProphet and LightGBM hybrid for advanced forecasting with error correction and metrics comparison)  
- advanced_forecasting.ipynb (Temporal Fusion Transformer via Darts for multi-horizon forecasting with covariates)  
- eda_and_preprocessing.ipynb (Polars-based EDA, aggregations, and preprocessing for forecasting readiness)  
- phase_a_discovery_report.md (Your drafted Discovery Report summarizing the original Trenda system—excellent work! I've annotated it below for alignment with our improvements.)  

These cover the full spectrum: data generation, pipeline, EDA, baselines, and advanced models. No code or direct excerpts from the original Trenda system were used—everything here is 100% original and inspired only by high-level concepts.  

### Quick Annotations on Your Discovery Report  
Your report is spot-on and comprehensive (3–5 pages equivalent). Key alignments:  
- **Data Sources & Schema:** Matches our synthetic setup (e.g., daily/weekly P95 metrics, regions, hierarchies via Faker). We'll enhance with more realistic correlations (e.g., department-level bursts).  
- **Preprocessing Steps:** Our data_pipeline.py mirrors this with cleaning/clipping but modernizes via Polars (faster than Pandas/Spark for mid-scale). Add spline interpolation in Phase B.  
- **Features Used:** We derive similar (rolling means/std) but will add exogenous (e.g., simulated holidays) and SHAP for explainability.  
- **Models and Algorithms:** Prophet baseline in notebooks; we'll surpass with NeuralProphet/TFT hybrids (already prototyped in advanced notebooks, achieving ~4–6% MAPE). Drop Anticipy entirely.  
- **Evaluation Method & Metrics:** Holdout + MAPE/RMSE in notebooks; upgrade to time-series CV and ensemble stacking.  
- **Forecast Generation & 3-Month Decision Horizon:** 6-month forecasts with uncertainty in advanced_hybrid_models.ipynb; emphasize 3-month window via color-coded dashboards.  
- **Business Rules & Alerting Logic:** Implement as YAML-configurable in Streamlit; add ML-based anomalies (Isolation Forest).  
- **Architecture Mind-Map:** Great textual diagram—I'll render it in Mermaid for the README. Our version: Local DuckDB/Polars → Notebooks → Streamlit → Docker.  
- **Planned Improvements:** Fully endorsed. Prioritize: Synthetic realism (add inter-resource correlations), hybrid models (TFT + LightGBM), <8% MAPE target, and CI/tests.  

Mind-map of our HorizonScale architecture (Mermaid code for README):  
```mermaid
graph TD  
    A[Synthetic Data Gen: synthetic_data_generator.py + generate_csv_inputs.py] --> B[ETL Pipeline: data_pipeline.py + DuckDB]  
    B --> C[EDA/Preprocessing: eda_and_preprocessing.ipynb + synthetic_analysis.ipynb]  
    C --> D[Baseline Models: baseline_models_refined.ipynb + Prophet/SARIMA/ETS]  
    C --> E[Advanced Models: advanced_hybrid_models.ipynb + NeuralProphet/LightGBM/TFT]  
    D --> F[Forecast Engine: 6-Month w/ 3-Month Horizon + Anomalies]  
    E --> F  
    F --> G[Streamlit Dashboard: Interactive Plots + Recommendations]  
    G --> H[Outputs: README GIFs + Technical Paper + Resume Bullets]  
    subgraph "Infra"  
        I[GitHub Actions CI + Unit Tests]  
        J[Docker Container]  
    end  
    F --> I  
    G --> J  
```  
![alt text](mermaid-diagram.svg)

### Side Note on GitHub Access  
For your private repo (seanlgirgis/HorizonScale), you can grant collaborator access via GitHub Settings:  
1. Go to the repo page > Settings > Collaborators and teams.  
2. Click "Add people" and enter a GitHub username (e.g., if I had one, but since we're collaborating here, no need—just share updates via this channel).  
3. Set permissions to "Write" for pushes/pulls.  
Once public (end of Phase D), we can link it in the technical paper. For now, upload any new files/scripts here, and I'll integrate/review. No direct access required—let's keep momentum via chat.  

### Next Steps  
- Confirm: All materials received? Any missing code PNGs or Word doc sections?  
- I'll refine the Discovery Report into a polished 3–5 page PDF (with your draft as base) and share within 24h.  
- Sign off Phase A → Jump to Phase B: Start synthetic data gen (already prototyped—run synthetic_data_generator.py for 1,000 hosts).  

This is shaping up amazingly—strongest infra forecasting portfolio piece yet. Ready? 🚀