# Copilot Progress Log

This file tracks detailed, timestamped progress for the Pops Studio Analytics Copilot project. Use it to log technical milestones, decisions, and implementation notes.

---

## Template for Progress Entries

### [YYYY-MM-DD] Title or Milestone
- **What was done:**
- **Files/Modules affected:**
- **Notes/Decisions:**

---

## Progress Entries 

### [2024-06-08] Initial Google Ads Summarization Script
- **What was done:** Created the first summarization script for Google Ads. It loads the latest parquet file, computes key metrics (impressions, clicks, cost, conversions, CTR, CPC, CPA), and outputs a Markdown summary to the summaries/ directory.
- **Files/Modules affected:** copilot/summarization/google_ads_summary.py, summaries/google_ads_summary.md (output)
- **Notes/Decisions:**
  - Script is modular and can be extended for weekly/monthly/rolling summaries.
  - Uses the same metrics as the Streamlit dashboard for consistency.
  - Output includes front-matter metadata for easy downstream use.

### [2024-06-11] Blog Metadata Crawler Implemented
- **What was done:** Implemented and tested a crawler that extracts blog post metadata (title, URL, publish date) from https://www.pops.studio/blogs/news and saves it to content/blog_metadata.json.
- **Files/Modules affected:** copilot/summarization/crawler.py, content/blog_metadata.json
- **Notes/Decisions:**
  - Selectors were customized to match the Shopify/POPS blog HTML structure.
  - The crawler found 6 blog posts and extracted all required fields.
  - Product metadata extraction is still generic and may need further customization for your shop structure.

### [2024-06-11] Multi-Category Product Metadata Crawler Implemented
- **What was done:** Implemented and tested a crawler that extracts product metadata (title, URL, category) from all major product categories and saves it to content/product_metadata.json.
- **Files/Modules affected:** copilot/summarization/crawler.py, content/product_metadata.json
- **Notes/Decisions:**
  - The crawler now iterates over all relevant Shopify collection pages.
  - 29 products were found across 6 categories.
  - Price extraction is set up but currently returns null; can be refined with a price HTML snippet.

### [2024-06-12] OpenAI Embedding Pipeline Integrated
- **What was done:** Updated the vector indexing script to use OpenAI's text-embedding-ada-002 model with the new openai>=1.0.0 API. The script now loads Markdown summaries, chunks them, generates real embeddings, and prepares for vector DB integration.
- **Files/Modules affected:** copilot/retrieval/vector_index.py, .env (for API key)
- **Notes/Decisions:**
  - The embedding pipeline is now fully functional and outputs real embeddings for summary chunks.
  - The next step is to integrate a vector database for semantic retrieval.
  - **Qdrant** is a recommended choice: it is open source, can be self-hosted for free, and also offers a managed cloud service with a generous free tier. No paid plan or subscription is required for local or small-scale cloud use.

### [2024-06-13] Switched Vector Retrieval from Qdrant to Chroma
- **What was done:**
  - Replaced Qdrant with ChromaDB for local vector storage and retrieval.
  - Updated indexing and query scripts to use Chroma's Python API.
  - Simplified setup: no Docker or server required, persistence handled automatically.
  - Added and tested end-to-end retrieval with Chroma, confirming correct results.
- **Files/Modules affected:**
  - copilot/retrieval/vector_index.py
  - copilot/retrieval/vector_query.py
  - copilot/tests/test_vector_retrieval.py
- **Notes/Decisions:**
  - Chroma is now the default for local/dev workflows.
  - Chroma's persistent mode is used for durability.
  - No external services or containers are required for development.
  - If needed, can switch back to Qdrant or another vector DB for production.

### [2024-06-13] Switched to File-Based Vector Storage (Chroma Persistence Unreliable)
- **What was done:**
  - Replaced ChromaDB with a simple, robust file-based vector storage system using numpy and scikit-learn.
  - `vector_index.py` now saves embeddings as `.npy` and metadata as `.pkl` in a `vector_storage/` directory.
  - `vector_query.py` loads these files and uses cosine similarity for semantic search.
  - Updated test script for the new storage approach.
- **Files/Modules affected:**
  - copilot/retrieval/vector_index.py
  - copilot/retrieval/vector_query.py
  - copilot/tests/test_vector_retrieval.py
- **Notes/Decisions:**
  - This approach is Python-version agnostic, dependency-free, and easy to debug.
  - **Current issue:** After running the indexer, the `vector_storage/` directory and output files are not present, despite the script reporting successful processing and upsert. This suggests a path-handling or directory-creation bug in the new implementation. Debugging is ongoing to ensure reliable file output before proceeding to query and test scripts.

### [2024-06-13] Python/Chroma Persistence Issue Diagnosed
- **What was done:**
  - Discovered that ChromaDB persistence does not work with Python 3.13.0 (pre-release).
  - Ran multiple tests: indexer upserts, inspector scripts, and direct Chroma upserts—all failed to create a persistent `chroma_db` directory.
  - Confirmed that Chroma's in-memory operations work, but nothing is saved to disk between runs.
- **Diagnosis:**
  - Chroma officially supports Python 3.8–3.11. Persistence is broken on Python 3.13.0.
  - Minimal test scripts confirmed the issue is with Python version compatibility, not project code.
- **Next steps:**
  - Install Python 3.11 via Homebrew.
  - Create a new virtual environment with Python 3.11.
  - Reinstall all dependencies and migrate the project to the supported version.
  - Retest Chroma persistence and resume development once fixed.
- **Files/Modules affected:**
  - All Chroma-based scripts (indexer, query, tests)
- **Notes/Decisions:**
  - This migration is required for reliable vector search and semantic retrieval.

### [2025-06-15] Rolling-Window Summaries, Structured Logging, and LLM Narratives
- **What was done:**
  - Refactored GA4, Search Console, and Google Ads summarisation scripts to generate 7-, 90-, and 365-day Markdown reports instead of single "latest" snapshots.
  - Added `window_days` field to front-matter metadata.
  - Introduced a central `copilot/utils/logger.py` and added structured logging (+ env var `COPILOT_LOG_LEVEL`).
  - Each summary script now calls OpenAI (`gpt-3.5-turbo-0125`) to append a concise narrative paragraph; skips gracefully if no API key.
  - Unit-tests updated to use real parquet data; synthetic fixtures removed.
  - Added fallback NumPy cosine-similarity implementation when scikit-learn is not installed.
- **Files/Modules affected:**
  - copilot/summarization/ga4_summary.py
  - copilot/summarization/search_console_summary.py
  - copilot/summarization/google_ads_summary.py
  - copilot/utils/logger.py
  - copilot/retrieval/vector_query.py
  - copilot/tests/test_*_metrics.py
- **Notes/Decisions:**
  - Rolling windows better match business reporting cadence.
  - Logging standardised across modules; log level configurable.
  - Narrative generation provides quick human-readable insights for internal users.

### [2025-06-15] Blog Crawler Pagination & Robustness
- **What was done:**
  - Reworked `crawler.py` to include retry logic, timeout, and exponential back-off (`fetch_url`).
  - Added full pagination support for `/blogs/news` archive using `rel="next"` detection.
  - Integrated structured logging; reports total posts crawled.
  - Product crawling code retained but disabled by default; main script now only refreshes `blog_metadata.json`.
  - Added real-site PyTest that verifies at least one post is returned (skips if network unavailable).
- **Files/Modules affected:**
  - copilot/summarization/crawler.py
  - copilot/tests/test_crawler.py
- **Notes/Decisions:**
  - Product metadata deemed low priority for current analytics goals; can be re-enabled later.
  - Keeping crawler lightweight avoids unnecessary traffic and speeds up CI.

### [2025-06-15] FastAPI Backend & Vector Retrieval Integration
- **What was done:**
  - Implemented `copilot/backend/app.py` FastAPI service with `/chat` endpoint.
  - Endpoint retrieves context chunks via file-based vector search, builds prompt, and calls OpenAI chat model to produce actionable Ads/SEO recommendations.
  - Added Pydantic schemas (`ChatRequest`, `ChatResponse`, `ContextChunk`).
  - Created `copilot/tests/test_backend_chat.py` integration test using real OpenAI API; passes.
  - Updated vector index with overlap chunking, deterministic IDs, and idempotent upsert logic.
- **Files/Modules affected:**
  - copilot/backend/app.py, copilot/backend/__init__.py
  - copilot/retrieval/vector_index.py, vector_query.py
  - copilot/tests/test_backend_chat.py
- **Notes/Decisions:**
  - Backend ready for UI integration; exposes robust analytics-focused chat interface.
  - Vector index now idempotent and more context-aware with overlapping chunks.

### [2025-06-15] Deployment Issues Observed During UI/Backend Launch
- **Symptoms:**
  - FastAPI backend fails to start via Uvicorn; traceback shows `copilot/backend/__init__.py` containing leftover placeholder text (`CREATE`) which causes `NameError` / `SyntaxError` loops under the auto-reloader.
  - Streamlit Copilot views raise `SyntaxError: from __future__ imports must occur at the beginning of the file` because `__future__` import was not first line.
  - Selecting Copilot views previously triggered a `TypeError: PosixPath / NoneType` when data-loading logic still executed before early return.
- **Immediate Fixes Applied:**
  - Removed placeholder content and replaced `copilot/backend/__init__.py` with an empty file containing only `pass`.
  - Moved `from __future__ import annotations` to the very first line of `streamlit/views/copilot_view.py`.
  - Added early return in `streamlit/main.py` to bypass data-loading for Copilot views.
- **Outstanding:**
  - Need to fully restart backend and Streamlit after these file edits to confirm clean launch (no reloader loops).
  - Verify vector index exists at repo root before backend is queried.
  - Once confirmed, integrate success message into README for devs. 