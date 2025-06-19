# GA-4 Behavioural-Signal Analysis – June 2025

This note captures the exploratory work we ran in `analysis/user_behavior_analysis.py` to discover which onsite micro-engagements correlate most strongly with **Level-5 intent** (sessions that include any of: `add_to_cart`, `view_cart`, `form_start`, `begin_checkout`).  
The goal is to feed hard evidence into the Engagement-Pyramid redesign and the LLM reporting pipeline.

---
## 1. Current findings (parquet export up to 6 Jun 2025)
* Sessions analysed ........................................ **1 270**  
* Level-5 intent sessions .................................. **134**  
* Logistic-regression AUC .................................. **0.84**

### Descriptive lifts
| Behaviour                       | Lift vs non-L5 |
|---------------------------------|----------------:|
| ≥ 1 FAQ click                   | **5.3 ×** |
| ≥ 1 Gallery click               | 2.7 × |
| ≥ 3 Page views                  | 1.5 × |
| ≥ 1 On-site search              | 0.5 × (negative) |

### Top predictive features (log-reg coefficients)
| Feature                       | Direction | Strength |
|--------------------------------|-----------|-----------|
| traffic_medium = **cpc**       | ↑ | +1.52 |
| geo_country = **Poland**       | ↑ | +0.88 |
| device_category = **tablet**   | ↑ | +0.72 |
| n_pageviews (numeric)          | ↑ | +0.66 |
| n_gallery (numeric)            | ↑ | +0.53 |
| n_faq (numeric)                | ↑ | +0.38 |
| traffic_source = pinterest.com | ↑ | +0.46 |
| (many others < 0.30 omitted)   |   |   |

Interpretation:
* **FAQ interaction is the single strongest behavioural predictor** of high-intent sessions.
* Deep gallery engagement and page-depth also matter.
* Ads traffic (medium `cpc`) delivers disproportionate numbers of Level-5 sessions – a metric we should surface (cost / L5-session).
* On-site search currently *negatively* correlates; likely UX or relevance issue.

---
## 2. How to rerun / update the analysis

1. **Activate environment** (repo root):
   ```bash
   python -m venv .venv       # if not created
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
   The script requires `scikit-learn`; it is already listed.

2. **Ensure GA-4 parquet is up to date** (optional but recommended):
   ```bash
   PYTHONPATH=. python data/GA4_fin_v5.py smart_sync
   ```
   This pulls any new `events_YYYYMMDD` tables into `data_repo/ga4/analytics_events_final/`.

3. **Run the analysis**:
   ```bash
   python analysis/user_behavior_analysis.py
   ```
   The script will:
   1. Load all parquet files.
   2. Aggregate to session-level, derive behavioural counts.
   3. Print descriptive lifts and logistic-regression coefficients.

4. **Adjust script parameters**:
   * Edit `signals` list (for lift table) inside the script.
   * Modify `cat_cols` / `num_cols` lists to add features.
   * Change positive-class definition by modifying the `level5_intent` lambda.

---
## 3. Next integration steps
* **Engagement Pyramid** – define Level-3 as "≥1 FAQ OR ≥1 gallery OR ≥3 pageviews"; Level-4 adds `cpc` traffic or tablet device.
* **Summary JSON** – write lifts & top-coefficients into GA-4 summary JSON so the LLM prompt can cite them.
* **Report metrics** – add "Cost per L5 session" to Ads summary and combined summary.
* **LLM prompt tweaks** – instruct the model to emphasise FAQ optimisation and gallery UX, and to flag the onsite search issue.

---
**Last updated:** 13 Jun 2025 