# POPS Analytics – Data Pipeline, Copilot & Dashboard

POPS Analytics is an end-to-end analytics stack for the pops.studio e-commerce shop.
It collects raw data (GA4, Google Ads, Search Console), stores processed parquet
files, and exposes insights through two user-facing components:

* **Streamlit dashboard** (`streamlit/`) – interactive GA4 / Ads / Search Console
  exploration.
* **Copilot** (`copilot/`) – FastAPI + LLM assistant that answers questions and
  generates weekly reports using vector-search over Markdown summaries.

---
## Directory layout (high-level)

```
.
├── copilot/                  # LLM assistant (backend, retrieval, etc.)
│   ├── backend/              # FastAPI app (chat endpoint)
│   ├── frontend/             # Streamlit widgets that call the backend
│   ├── retrieval/            # Vector index + query helpers (OpenAI embeddings)
│   ├── summarization/        # Scripts that convert parquet → Markdown summaries
│   ├── changes/              # Snapshot + changelog utilities
│   ├── content/              # Crawled blog/product metadata (JSON)
│   ├── summaries/            # Generated Markdown KPI summaries (LLM context)
│   └── vector_storage/       # `embeddings.npy` + `metadata.pkl` for similarity search
│
├── streamlit/                # GA4 dashboard
│   ├── main.py               # entry-point (`streamlit run …`)
│   ├── utils.py              # data-loading helpers
│   └── views/                # per-view render modules
│
├── data/                     # Extraction scripts (BigQuery, Ads, GSC)
├── data_repo/                # Parquet output organised by dataset/table/month
├── utils/                    # Shared logging / error-handling helpers
├── config.py                 # Centralised env-var config
└── tests + others …
```

---
## Quick-start (full stack)

1. **Python env**  
  *(Keep the virtual-env outside the repo to avoid git noise)*
  ```bash
  python -m venv ~/venvs/pops-analytics
  source ~/venvs/pops-analytics/bin/activate
  pip install -r requirements.txt            # plus any extras you need
  ```

2. **Environment variables**  
  Create a `.env` file in the repo root (or export vars in your shell):
  ```text
  # --- mandatory ---
  OPENAI_API_KEY=sk-…
  COPILOT_BACKEND_URL=http://localhost:8000

  # --- optional / data-pipeline ---
  GOOGLE_APPLICATION_CREDENTIALS=/path/to/ga4-service-account.json
  ```

3. **Generate KPI summaries (30-day window)**  
  These scripts read parquet in `data_repo/` and write Markdown to
  `copilot/summaries/`. Run them whenever you refresh the data.
  ```bash
  python copilot/summarization/ga4_summary.py
  python copilot/summarization/search_console_summary.py
  python copilot/summarization/google_ads_summary.py
  ```

4. **Build / refresh the vector index** (required for chat & context search)
  ```bash
  python copilot/retrieval/vector_index.py
  ```

5. **Start the Copilot backend** (FastAPI)  
  Runs on port 8000 by default – adjust `COPILOT_BACKEND_URL` if you change it.
  ```bash
  uvicorn copilot.backend.app:app --reload --port 8000
  ```

6. **Launch the Streamlit dashboard**  
  ```bash
  streamlit run streamlit/main.py
  ```

  • Sidebar → *Copilot Report* uses the latest **30-day** summaries.  
  • Sidebar → *Copilot Chat* talks to the backend you started in step 5.

7. *(Optional)* **Text-to-speech**  
  The "Listen to this report" expander in Copilot Report calls the OpenAI
  Speech API; make sure your `OPENAI_API_KEY` has access to the
  `tts-1` model.

---
## How the LLM pieces fit together

1. **Summaries** – scripts in `copilot/summarization/` read parquet, compute KPIs
   and ask GPT-3.5 for a short narrative.  The resulting `.md` files land in
   `copilot/summaries/`.
2. **Vector index** – `vector_index.py` chunks those Markdown files, embeds them
   with OpenAI, and stores the numpy/pickle pair in
   `copilot/vector_storage/`.
3. **Chat endpoint** – `copilot/backend/app.py` embeds the user question, does a
   cosine-similarity search in the vector store, and sends a context-aware
   prompt to GPT-3.5.
4. **Streamlit Copilot front-end** – calls the backend for chat or assembles a
   multi-summary report via GPT-3.5 (`frontend/streamlit_view.py`).

---
## Tests
```bash
pytest copilot/tests
```
Covers vector indexing/query and live backend chat.

---
## House-keeping notes
• Virtual-env, caches, etc. are ignored via `.gitignore`.
• All generated artefacts (summaries, vector store) are reproducible and can be
  purged if space is needed. 