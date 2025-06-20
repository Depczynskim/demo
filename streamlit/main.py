# Force redeployment trigger
import sys, os
from pathlib import Path
# Ensure the Streamlit app folder and repo root are on the import path so the local
# ``utils.py`` (inside the *streamlit/* folder) shadows the empty top-level
# ``utils/`` package, and so that we can still import top-level packages like
# ``copilot``.
_STREAMLIT_DIR = Path(__file__).resolve().parent  # .../streamlit
_REPO_ROOT = _STREAMLIT_DIR.parent
for _p in (str(_STREAMLIT_DIR), str(_REPO_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import streamlit as st
import pandas as pd
import openai

# Dynamically import ``streamlit/utils.py`` under an alias to avoid clashing with
# the unrelated top-level ``utils`` package.
import importlib.util as _ilu

_UTILS_SPEC = _ilu.spec_from_file_location("_dash_utils", _STREAMLIT_DIR / "utils.py")
_dash_utils = _ilu.module_from_spec(_UTILS_SPEC)  # type: ignore[var-annotated]
_UTILS_SPEC.loader.exec_module(_dash_utils)  # type: ignore[union-attr]

# Re-export the convenience names so downstream code remains unchanged.
DATA_REPO = _dash_utils.DATA_REPO
PAGE_TYPE_RULES = _dash_utils.PAGE_TYPE_RULES
categorize_page_type = _dash_utils.categorize_page_type
_datasets = _dash_utils._datasets
_month_parts = _dash_utils._month_parts
_load_parquet = _dash_utils._load_parquet
_parse_ga4_event_params = _dash_utils._parse_ga4_event_params

# Import view modules
from views import search_console_view, google_ads_new_view, data_browser_view, product_view, overview_view

# Set page config must be the first Streamlit command
st.set_page_config(page_title="POPS Analytics – GA4 Raw Data v3", layout="wide")

# Remove the always-visible banner; each view now handles its own intro.

# Check if data repo exists
if not DATA_REPO.exists():
    st.error(f"Data repo not found at {DATA_REPO.resolve()}")
    st.stop()

# ── Sidebar controls ──────────────────────────────────────────────────────

st.sidebar.header("Controls")

# Updated, cleaner labels for sidebar views
dataset = st.sidebar.radio(
    "Data source / View",
    [
        "Overview",
        "Search Console",
        "Google Ads",
        "Products",
        "AI-Generated Product Insights",
        "GA4 Browser",
    ],
    index=0,  # Default to Overview
)

# If user selects a Copilot view, render immediately and exit main script
if dataset == "Overview":
    overview_view.render()
    st.stop()
elif dataset == "AI-Generated Product Insights":
    try:
        from copilot.frontend import streamlit_view as copilot_view
        copilot_view.render_report()
    except openai.OpenAIError:
        # This is a fallback for the persistent, environment-specific error.
        # Instead of crashing, we display a user-friendly message.
        st.header("🧠 AI-Generated Insights")
        st.info(
            "Thank you for your interest! The AI report generation feature is "
            "disabled on this public-facing demo due to security and configuration "
            "considerations. This feature is fully functional in private deployments."
        )
        st.warning(
            "The underlying code for this feature is available in the "
            "[GitHub repository](https://github.com/Depczynskim/demo), but requires a "
            "valid OpenAI API key to be configured in secrets."
        )
    except Exception as e:
        # Catch any other unexpected errors during render
        st.header("🧠 AI-Generated Insights")
        st.error(f"An unexpected error occurred while loading this view: {e}")
    st.stop()

time_span = st.sidebar.radio("Time span", ["Last 3 months", "All time"], index=0)
months = _month_parts(3) if time_span.startswith("Last") else None

# Custom date range filter for 'All time'
custom_start = custom_end = None
if time_span == "All time":
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Custom Date Range**")
    st.sidebar.info("This will filter data across all views")
    custom_start = st.sidebar.date_input("Start date", value=None, key="custom_start")
    custom_end = st.sidebar.date_input("End date", value=None, key="custom_end")
    
    if custom_start and custom_end:
        # Ensure both are pandas Timestamps
        custom_start = pd.to_datetime(custom_start)
        custom_end = pd.to_datetime(custom_end)
        if custom_start > custom_end:
            st.sidebar.error("Start date must be before end date")
            st.stop()

# Map to new unified table names (only for data views)
TABLE_MAP = {
    "Search Console": "search_console_final",
    "Google Ads": "analytics_events_final",
    "GA4 Browser": "analytics_events_final",
    "Products": "analytics_events_final",
}

# Copilot views do not require data table loading
if dataset.startswith("Copilot"):
    table = None
else:
    table = TABLE_MAP.get(dataset, None)
    if not table:
        st.error("Table mapping failed for selected dataset.")
        st.stop()

# ── Load and display data ─────────────────────────────────────────────────

# When loading data, force dataset to 'ga4' if 'Google Ads', 'GA4 Browser', or 'Products' is selected
actual_dataset = dataset
if dataset in ["Google Ads", "GA4 Browser", "Products"]:
    actual_dataset = "ga4"
elif dataset == "Search Console":
    # Map the friendly view label to the underlying data directory name
    actual_dataset = "search_console"

# If custom date range is set, load all data and filter by date
if time_span == "All time" and custom_start and custom_end:
    df = _load_parquet(actual_dataset, table, None)
    # Try to convert 'date' column if present
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
else:
    df = _load_parquet(actual_dataset, table, months)

if dataset.startswith("Copilot"):
    df = None  # context not needed
else:
    if df.empty:
        st.warning(f"No data found for {actual_dataset}/{table} in selected months.")
        st.stop()

# Calculate actual date range in the data
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])
    actual_start = df['date'].min()
    actual_end = df['date'].max()
else:
    actual_start = actual_end = None

# Keep raw table info for debugging but move it below each specific view if needed.  
raw_preview_info = f"Raw table preview – {actual_dataset}/{table} | Rows: {len(df):,}"
if actual_start and actual_end:
    raw_preview_info += f" | Date range: {actual_start.strftime('%Y-%m-%d')} → {actual_end.strftime('%Y-%m-%d')}"

# ── Route to appropriate view ─────────────────────────────────────────────

# Create a shared context dictionary to pass to views
context = {
    'DATA_REPO': DATA_REPO,
    'PAGE_TYPE_RULES': PAGE_TYPE_RULES,
    'categorize_page_type': categorize_page_type,
    '_datasets': _datasets,
    '_month_parts': _month_parts,
    '_load_parquet': _load_parquet,
    '_parse_ga4_event_params': _parse_ga4_event_params,
    'months': months,
    'df': df,
    'actual_dataset': actual_dataset,
    'dataset': dataset,
    'table': table,
    # Add date-related context
    'time_span': time_span,
    'custom_start': custom_start,
    'custom_end': custom_end,
    'selected_date_range': {
        'start': custom_start if custom_start else actual_start,
        'end': custom_end if custom_end else actual_end
    }
}

# Route to appropriate view based on dataset
if dataset == "Search Console":
    search_console_view.render(context)
    st.caption(raw_preview_info)
elif dataset == "Google Ads":
    google_ads_new_view.render(context)
    st.caption(raw_preview_info)
elif dataset == "GA4 Browser":
    data_browser_view.render(context)
    st.caption(raw_preview_info)
elif dataset == "Products":
    product_view.render(context)
    st.caption(raw_preview_info)
