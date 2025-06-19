# Copilot Directory Structure

This directory contains all code and resources related to the Pops Studio Analytics Copilot. Each subdirectory is modular, supporting a scalable and maintainable assistant system.

## Subfolders and Their Purpose

- **backend/**: FastAPI backend service for Copilot chat and analytics endpoints.
- **frontend/**: Streamlit UI components for Copilot (e.g., chat and report views).
- **summarization/**: Scripts and logic for generating analytics summaries (GA4, Google Ads, Search Console, etc.).
- **retrieval/**: Vector search, embedding, and retrieval logic for Copilot context.
- **automation/**: Automation scripts (e.g., changelog updates, scheduled jobs).
- **llm/**: LLM prompt/response logic and helpers.
- **memory/**: Chat/session memory management for Copilot conversations.
- **prompts/**: Prompt templates and prompt engineering assets.
- **utils/**: Copilot-specific utilities (logging, helpers, etc.).
- **tests/**: All Copilot-related unit and integration tests.
- **docs/**: Documentation, setup guides, and changelogs for Copilot.

## Usage Notes

- All Copilot code is importable as a package. Run scripts, tests, and Streamlit with `PYTHONPATH=$(pwd)` from the project root.
- Each folder contains only logic relevant to its domain, making it easy to find and maintain code.
- See `docs/` for setup and development guides.

## Supporting Directories

- **summaries/**: Output Markdown summaries for each data source.
- **content/**: Blog and product metadata (JSON files).
- **changes/**: Changelog and site update tracking.

---

*As you implement each Copilot feature, place the code in the appropriate subdirectory. Use the same modular principles as the rest of your codebase for maximum maintainability.*
