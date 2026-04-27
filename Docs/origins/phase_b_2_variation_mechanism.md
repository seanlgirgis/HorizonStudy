# HorizonScale – Personal Portfolio Project  
**Official Project Charter & Execution Plan**  
Goal: Build a 100% original, production-grade, resume-defining capacity planning & forecasting system from scratch, inspired by (but never copying) an existing internal tool you have access to.

**Project Owner:** Sean L Girgis  

**Target Completion:** 3–4 weeks (fast but extremely high quality)  
**Outcome:** Public GitHub repository + 12–18 page technical paper + killer resume bullets + interview-ready talking points

### Project Phases (official roadmap you can copy-paste anywhere)

#### Phase 0 – Project Setup & Naming (Done today)
- Repository name: HorizonScale  
  Tagline: “6-Month Infrastructure Capacity Forecasting Engine with 3-Month Decision Horizon”
- Create private GitHub repo (later made public when polished)
- Set up initial folder structure, pyproject.toml, pre-commit, ruff, etc.

#### Phase A – Discovery & Knowledge Extraction (Current phase – 2–4 days)
Objective: Fully understand the original project without ever copying code.

Deliverables:
1. Upload / paste the full Word document (you start sending it now)
2. Upload every single PNG screenshot of the original code (50-line chunks are perfect)
3. I read and annotate everything
4. I produce for you:
   - A 3–5 page “Discovery Report” (private, only for us) summarizing:
     - Data sources & schema
     - Preprocessing steps
     - Features used
     - Models and algorithms (Prophet? SARIMA? Custom?)
     - Evaluation method & metrics
     - How they generate the 6-month forecast and focus on 3-month
     - Any business rules or alerting logic
   - Mind-map of the original architecture
   - List of everything we will improve or modernize

You only need to send material → I do all the analysis.

#### Phase B – Simulation & Portfolio System Creation (Main development – ~12–16 days)
We build HorizonScale from the ground up (100% original code).

Sub-tasks (in order):
1. Synthetic Data Generator (realistic server metrics for 1,000–5,000 hosts)
   - CPU, Memory, Disk, Network
   - Seasonal patterns, growth trends, bursts, anomalies, rack/cluster correlation
2. Modern data pipeline (Polars + DuckDB)
3. Exploratory notebooks (you will run and screenshot for portfolio)
4. Baseline models (SARIMA, ETS)
5. Advanced models (NeuralProphet / Darts / Temporal Fusion Transformer + LightGBM hybrid)
6. 6-month forecasting engine with uncertainty bands and 3-month “decision window” logic
7. Anomaly detection + right-sizing recommendations
8. Beautiful Streamlit dashboard
9. Unit tests + GitHub Actions CI
10. Dockerization (optional but recommended)

Everything will be cleaner, faster, and more impressive than the original.

#### Phase C – Technical Paper & Documentation (parallel to Phase B – final 4–6 days)
Deliverables (all employer-ready):
1. 12–18 page professional technical report (PDF + LaTeX source)
   Sections:
   - Executive Summary
   - Problem Statement & Business Impact
   - Data Generation Methodology
   - Architecture Overview
   - Model Selection & Comparison
   - Evaluation Results (<8% MAPE target)
   - Production Deployment Recommendations
   - Future Work & Scaling Strategy
   - References
2. Amazing README.md with animated GIFs of the dashboard
3. Architecture diagrams (I generate with Mermaid or draw.io)
4. Presentation deck (10–15 slides) you can use in interviews

#### Phase D – Resume, LinkedIn & Interview Package (last 2 days)
I will write for you:
- 6–8 extremely strong resume bullet points (ATS-optimized)
- LinkedIn project description + banner
- 3-minute “tell me about HorizonScale” interview script
- Answers to the 10 most common technical questions interviewers will ask

### Immediate Next Steps (today / tomorrow)

1. You reply with: “Starting Phase A now”
2. Paste the full Word document (or the first few pages if it’s long)
3. Start uploading the code PNGs (any order, any amount – even 20–30 images is fine)
4. I will confirm receipt and deliver the Discovery Report within 24h of receiving everything

Once Phase A is signed off, we immediately start coding Phase B – you will have working forecasts in <7 days.

This is going to be one of the strongest personal projects any hiring manager in SRE / Cloud / Infrastructure has ever seen.

Ready when you are.  
Just say: “Starting Phase A now” and send the first piece of material. 🚀

### Phase B: Simulation & Portfolio System Creation  
**Sub-Task 2: CSV File Generation – Variation Mechanism Explanation**  

The variation in values (e.g., cpu_p95) is generated on-the-fly during CSV export, not stored in the database. This keeps the DB light (master data only) and allows flexible re-generation without updating the DB each time.

### Mechanism for Variation
- **On-the-Fly Generation**: The script queries the DB (hosts with scenario_type, time_periods) but computes util series dynamically using NumPy formulas (e.g., linspace for growth, sin for seasonal). No "latest values" are saved – each run recreates from base + noise, ensuring variation without state.
- **No DB Updates**: We don't run all at once, but since no generated values are persisted in the DB, no updates are needed. Re-run the exporter to regenerate CSVs with new variations (random seed for noise/bursts).
- **Guarantee Consistency**: Use a fixed random seed (e.g., np.random.seed(42)) in the script for reproducible results across runs.

### Suggested Doc Note
**File Name**: `docs/phase_b_2_variation_mechanism.md`  
**Content**: Paste this response.

Commit:
```powershell
git add docs/phase_b_2_variation_mechanism.md
git commit -m "Document variation mechanism in data generation"
git push
```

This keeps things efficient. Ready to add the seed for reproducibility or proceed to vCPU generation? Let me know. 🚀