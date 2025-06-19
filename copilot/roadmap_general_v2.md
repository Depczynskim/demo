# Copilot Revamp – High-Level Implementation Roadmap

> *A bird's-eye view: five macro steps from today's codebase to the hermeneutic, value-centric Copilot.*

---

### 1 Establish the *Valuable-Traffic* canon

• Design & agree the definition of *valuable* vs *rubbish* traffic (events, thresholds, conversion proxy).  
• Implement `valuable_classifier.py` → tag all GA4 sessions + persist `sessions_labeled_latest.parquet`.  
• Validate precision/recall; publish a markdown evaluation report.

---

### 2 Build subject data loaders

• Create modular loaders (`products.py`, `geo.py`, `campaigns.py`, `trends.py`).  
• Each outputs a **subject package**: metrics, 12-month history, valuable/rubbish split, top-N tables.  
• Replace legacy 30/90/365 scripts with a single 30-day rolling window (12-month depth).

---

### 3 Hermeneutic summarisation pipeline

• Step-A: Generate subject narratives via o3-mini + critic pass.  
• Step-B: Produce combined summary → feed back to revise subject narratives (1-2 loops).  
• Typed JSON + Markdown artefacts at each stage for determinism.

---

### 4 Recommendations & report composer

• Generate actionable, non-trivial recommendations per subject (with critic).  
• Concatenate subject narratives + combined summary + recs into `final_report.md`.  
• Streamlit: add Change-log widget, audio playback, and new Report view.

---

### 5 Service integration & deprecation

• Expose new report via existing FastAPI + Streamlit endpoints.  
• Add `/chat_sql` clarify-and-confirm flow.  
• Retire legacy 30/90/365 code paths once parity reached.  
• Define monitoring: valuable-label precision, critic-free pass rate, stakeholder feedback loop.

---

*End of high-level roadmap – each macro step can now be broken down into sprint tasks as needed.* 