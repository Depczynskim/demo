# Pops Studio — Analytics Copilot Setup (v2: Project-Aligned Roadmap)

This document provides a step-by-step plan to evolve the Pops Studio analytics system into an AI-powered copilot, based on the current state of the codebase and data infrastructure. It highlights what is already in place, what is missing, and the logical order for building out the assistant. **A testing phase is included after each major milestone.**

---

## 1. Current State Overview

- **Data Extraction & Storage:**  
  - Automated `.parquet` exports for GA4, Google Search Console, and Google Ads are present and organized in `data_repo/`.
  - No Pinterest data pipeline is present yet.
- **Dashboards:**  
  - Streamlit dashboards exist for data exploration and visualization.
- **Configuration:**  
  - Centralized in `config.py` with support for environment variables and feature flags.
- **Missing:**  
  - No summary generation, vector search, chat memory, backend copilot service, or automation pipeline.
  - No website/blog crawler, changelog, or content metadata store.

---

## 2. Key Business Questions for the Copilot (Driven by Existing Metrics)

The copilot should be able to answer the following questions, using the metrics and analytics already implemented in the Streamlit app as the primary data source:

1. **How should we modify Google Search campaigns, times, and keyword bids?**
   - The copilot should:
     - Analyze current campaign, ad group, and keyword performance (impressions, clicks, cost, conversions, CTR, CPC, CPA).
     - Reference current settings (bids, schedules, targeting) if available.
     - Suggest bid adjustments, time-of-day changes, or pausing underperforming elements based on trends.
     - *Note: Exporting current settings from Google Ads API may be required for full recommendations.*

2. **What blog posts should we publish next, and can the LLM create deep research prompts?**
   - The copilot should:
     - Analyze Search Console queries and GA4 landing pages to identify trending topics.
     - Compare current blog topics to trending queries (gap analysis).
     - Generate prompts for the LLM to research and draft posts on high-potential topics, including SEO keywords and rationale.
     - *Requires blog metadata and crawler for full automation.*

3. **How has the overall position of the shop changed, and what were the drivers (using GA4, Google Ads, Search Console)?**
   - The copilot should:
     - Summarize changes in average position, clicks, impressions, and CTR over time.
     - Use driver analysis (impact score, delta values) to explain what pages/queries/campaigns contributed most to changes.
     - Synthesize across GA4, Ads, and Search Console for a holistic view.

4. **What are the top performing products, and what is the typical user behaviour for them?**
   - The copilot should:
     - Identify top products by sales, views, or conversions (GA4 data).
     - Analyze user behaviour: session paths, repeat visits, conversion funnels.
     - Segment by country, city, device, and channel.
     - Track changes in user origin and suggest improvements (e.g., new markets, device optimization).

5. **Where are users coming from, did that change with time, and do the data suggest any improvements?**
   - The copilot should:
     - Use GA4 geo and channel data to show shifts in user base over time.
     - Highlight significant changes and suggest actionable improvements.

6. **What are Pinterest trends telling us about our relevance, and should we be aware of any new trends or add new products?**
   - The copilot should:
     - Analyze Pinterest trend and autocomplete data (once pipeline is implemented).
     - Identify rising topics or keywords and cross-reference with product catalog.
     - Suggest new products or categories based on trend analysis.
     - *Requires Pinterest data pipeline for full coverage.*

---

## 3. Mapping Business Questions to Existing Metrics & Code

| Business Question | Metrics/Logic in Code | Additional Data Needed |
|-------------------|----------------------|-----------------------|
| Google Ads campaign optimization | Impressions, clicks, cost, conversions, CTR, CPC, CPA, time series, top performers (see `render_performance_summary`, `render_performance_analysis` in `google_ads_new_view.py`) | Export current settings (bids, schedules) from Google Ads API |
| Blog post suggestions | Search Console queries, GA4 landing pages, blog metadata (if crawler implemented) | Blog crawler, metadata extraction |
| Shop position & drivers | Average position, clicks, impressions, CTR, impact score, driver analysis (see `render_overview`, `get_comparison_metrics` in `search_console_view.py`) | None (core logic present) |
| Top products & user behaviour | Product-level GA4 metrics, user segmentation, timing analysis (see `product_view.py`) | None (core logic present) |
| User origin & improvement | GA4 geo/channel data, time series, segmentation | None (core logic present) |
| Pinterest trends & product ideas | (Planned) Pinterest trend/autocomplete data | Pinterest data pipeline |

---

## 4. Logical Roadmap for Copilot Development (with Testing)

### Step 1: Data Summarization Scripts
- **Goal:** Generate weekly, 3-month, and 12-month summaries for each data source (GA4, GSC, Ads).
- **Actions:**
  - Write Python scripts to load `.parquet` files and compute KPIs/trends.
  - Output `.md` summary files in a new `/summaries/` directory.
  - Use a consistent structure with front-matter metadata.
  - Integrate LLM for narrative summary writing.
- **Testing:**
  - Write unit tests for KPI/trend calculation functions.
  - Manually verify that `.md` files are generated and formatted correctly.
  - Check that summaries update as new data arrives.

---

### Step 2: Website & Blog Crawler
- **Goal:** Keep the copilot aware of site/blog/product content.
- **Actions:**
  - Build a lightweight crawler to extract blog titles, URLs, publish dates, and product/category info.
  - Save results to `/content/blog_metadata.json` and `/content/product_metadata.json`.
- **Testing:**
  - Write unit tests for parsing and extraction logic.
  - Manually inspect the JSON output for accuracy and completeness.
  - Test crawler on new/changed blog and product pages.

---

### Step 3: Changelog & Metadata Tracking
- **Goal:** Track site changes and blog metadata for LLM context.
- **Actions:**
  - Create `/changes/site_updates.md` and update it manually or via script.
  - Ensure blog/product metadata is refreshed weekly.
- **Testing:**
  - Manually verify changelog and metadata files are updated as expected.
  - Add tests to check for correct file structure and recent updates.

---

### Step 4: Vector Search Index
- **Goal:** Enable semantic retrieval of summaries and content.
- **Actions:**
  - Integrate a vector database (e.g., Qdrant).
  - Write a script to chunk `.md` summaries and content, generate embeddings, and index them.
  - Store metadata (file, date range, etc.) with each chunk.
- **Testing:**
  - Write unit tests for chunking and embedding logic.
  - Test retrieval with sample queries to ensure relevant results.
  - Validate that new/updated summaries are indexed correctly.

---

### Step 5: Copilot Backend Service
- **Goal:** Orchestrate chat, memory, and retrieval.
- **Actions:**
  - Build a FastAPI (or similar) backend to:
    - Accept chat input from UI.
    - Retrieve context from vector DB.
    - Store short-term and long-term chat memory (e.g., in Redis or files).
    - Call the LLM and return responses.
    - Log all Q&A for future context.
  - Note: Start with a simple file-based memory if Redis is not yet set up.
- **Testing:**
  - Write integration tests for API endpoints.
  - Test chat flow end-to-end (UI → backend → LLM → response).
  - Simulate memory and retrieval scenarios.

---

### Step 6: Streamlit Chat UI Integration
- **Goal:** Connect the dashboard to the copilot backend.
- **Actions:**
  - Add a chat interface to the Streamlit app.
  - Connect to the backend via HTTP.
  - Display retrieved context and LLM responses.
- **Testing:**
  - Manually test chat UI for usability and correctness.
  - Add automated UI tests if possible (e.g., with Selenium or Playwright).

---

### Step 7: Weekly Automation Pipeline
- **Goal:** Keep all data, summaries, and indexes up to date.
- **Actions:**
  - Write a script or use a scheduler (e.g., cron, GitHub Actions) to:
    - Refresh data exports.
    - Run the summarization and crawling scripts.
    - Update the vector index.
    - Update changelog and metadata files.
- **Testing:**
  - Test the pipeline end-to-end in a staging environment.
  - Add logging and alerting for failures.
  - Verify that all outputs are refreshed as expected.

---

### Step 8: (Optional) Live Data Analysis Functions
- **Goal:** Allow the copilot to run safe, predefined analytics on fresh data.
- **Actions:**
  - Expose specific Python functions for the LLM to call (e.g., group-by, pivot, plot).
  - Ensure strict security and scope.
- **Testing:**
  - Write unit tests for each exposed function.
  - Test LLM integration with mock/fake data.

---

### Step 9: (Optional) Semantic Knowledge Graph
- **Goal:** Track relationships (e.g., product → category, blog → keyword).
- **Actions:**
  - Store in a JSON file, updated weekly.
- **Testing:**
  - Write tests to ensure correct relationship extraction and storage.
  - Manually inspect the knowledge graph for accuracy.

---

## 5. Prioritization & Milestones

1. Summarization scripts (enables LLM context and retrieval)
2. Crawling & metadata (enables content/context awareness)
3. Changelog (enables change tracking)
4. Vector search (enables semantic Q&A)
5. Backend service (enables chat/memory/orchestration)
6. UI integration (enables user interaction)
7. Automation (keeps everything fresh)
8. Live analysis & knowledge graph (advanced features)

---

## 6. Example LLM Prompts Using Your Metrics

- "Based on the last 30 days, which Google Ads keywords have the highest CPA and lowest conversion rate? Suggest bid adjustments."
- "What blog topics are trending in Search Console queries but not yet covered on our blog?"
- "Summarize the main drivers of change in our shop's average search position this month."
- "List the top 5 products by conversion rate and describe typical user paths leading to purchase."
- "Analyze Pinterest trends for the last week and suggest if we should add new products in [category]."

---

## 7. Next Steps

- [ ] Create `/summaries/`, `/content/`, and `/changes/` directories.
- [ ] Implement and test summarization scripts.
- [ ] Build and schedule the crawler.
- [ ] Set up vector DB and indexing.
- [ ] Develop the backend and connect the UI.
- [ ] Automate the pipeline.

---

## 8. Recommended Modular Directory Structure for Copilot Code

To ensure maintainability and scalability, all copilot-related code should be placed in a dedicated top-level directory (e.g., `copilot/`). This directory should be organized into subdirectories, mirroring the modular approach used elsewhere in the project.

**Recommended Structure:**

```
copilot/
  ├── backend/           # FastAPI or other backend service code
  ├── llm/               # LLM prompt templates, context builders, and response parsers
  ├── retrieval/         # Vector search, embedding, and context retrieval logic
  ├── summarization/     # Scripts for generating and updating summaries
  ├── automation/        # Pipeline and scheduling scripts
  ├── memory/            # Chat/session memory management
  ├── prompts/           # Prompt templates and prompt engineering logic
  ├── tests/             # Unit and integration tests for copilot modules
  └── utils/             # Copilot-specific utilities (can import from global utils if needed)
```

- **backend/**: Handles API endpoints, orchestration, and communication with the UI.
- **llm/**: Contains logic for building prompts, calling the LLM, and parsing responses.
- **retrieval/**: Manages vector search, chunking, and embedding of summaries/content.
- **summarization/**: Scripts for generating Markdown summaries from data.
- **automation/**: Handles scheduled tasks (e.g., weekly updates).
- **memory/**: Implements chat/session memory (file-based, Redis, etc.).
- **prompts/**: Stores reusable prompt templates for different business questions.
- **tests/**: All copilot-related tests.
- **utils/**: Copilot-specific helpers (can also use/import from the main `utils/`).

**Rationale:**
- Keeps copilot logic separate from analytics and data extraction.
- Makes it easy to test, refactor, and extend copilot features.
- Allows for clear ownership and boundaries between analytics and AI/assistant logic.

*As you implement each copilot feature, place the code in the appropriate subdirectory. Use the same modular principles as the rest of your codebase for maximum maintainability.*

---

*This roadmap is designed for incremental, testable progress. Adjust as needed for your environment and priorities.*

---

*End of instructions*
