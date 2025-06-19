import os
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import streamlit as st

# Get the repository root directory by navigating up from the current file's location
_REPO_ROOT = Path(__file__).resolve().parents[1]

# Define the data repo path relative to the repository root
DATA_REPO = _REPO_ROOT / "data_repo"

# ── 2 · Configurable page type rules ──────────────────────────────────────────
PAGE_TYPE_RULES = {
    "Homepage": [r"/$", r"home"],
    "Product Pages": [r"product", r"vanity", r"swing"],
    "FAQ/Help": [r"faq", r"help"],
    "Contact": [r"contact"],
    "About": [r"about"],
    "Blog/Articles": [r"blog", r"article"],
}

def categorize_page_type(page_location):
    if pd.isna(page_location):
        return "Unknown"
    page_location = str(page_location).lower()
    for page_type, patterns in PAGE_TYPE_RULES.items():
        for pat in patterns:
            if re.search(pat, page_location):
                return page_type
    return "Other"

# ── 3 · Cached helpers ────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _datasets() -> list[str]:
    """Return available datasets (google_ads, search_console, ga4)."""
    return sorted([p.name for p in DATA_REPO.iterdir() if p.is_dir()])

@st.cache_data(show_spinner=False)
def _month_parts(last_n: int = 3) -> list[str]:
    """Return a list of the last N months in YYYYMM format."""
    today = datetime.today().replace(day=1)
    return [(today - timedelta(days=30 * i)).strftime("%Y%m") for i in range(last_n)]

@st.cache_data(show_spinner=True)
def _load_parquet(dataset: str, table: str, months: list[str] | None) -> pd.DataFrame:
    base = DATA_REPO / dataset / table
    files = list(base.glob("**/*.parquet"))
    if months:
        files = [f for f in files if any(f"report_month={m}" in f.parts for m in months)]
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)

@st.cache_data(show_spinner=True)
def _parse_ga4_event_params(df: pd.DataFrame) -> pd.DataFrame:
    """Parse GA4 event_params_json to extract useful parameters."""
    if 'event_params_json' not in df.columns:
        return df

    def extract_param(params_json, param_name):
        try:
            if pd.isna(params_json):
                return None
            params = json.loads(params_json)
            for param in params:
                if param.get('key') == param_name:
                    value = param.get('value', {})
                    return (
                        value.get('string_value') or
                        value.get('int_value') or
                        value.get('float_value') or
                        value.get('double_value')
                    )
            return None
        except Exception as e:
            return None

    # Extract key parameters
    df['page_location'] = df['event_params_json'].apply(lambda x: extract_param(x, 'page_location'))
    df['page_title'] = df['event_params_json'].apply(lambda x: extract_param(x, 'page_title'))
    df['page_referrer'] = df['event_params_json'].apply(lambda x: extract_param(x, 'page_referrer'))
    df['engagement_time_msec'] = df['event_params_json'].apply(lambda x: extract_param(x, 'engagement_time_msec'))
    df['click_element'] = df['event_params_json'].apply(lambda x: extract_param(x, 'click_element'))
    df['click_text'] = df['event_params_json'].apply(lambda x: extract_param(x, 'click_text'))
    df['click_url'] = df['event_params_json'].apply(lambda x: extract_param(x, 'click_url'))
    df['section'] = df['event_params_json'].apply(lambda x: extract_param(x, 'section'))
    # Product/FAQ specific fields
    df['ecomm_prodid'] = df['event_params_json'].apply(lambda x: extract_param(x, 'ecomm_prodid'))
    df['item_id'] = df['event_params_json'].apply(lambda x: extract_param(x, 'item_id'))
    df['faq_question'] = df['event_params_json'].apply(lambda x: extract_param(x, 'faq_question'))
    df['faq_topic'] = df['event_params_json'].apply(lambda x: extract_param(x, 'faq_topic'))
    df['faq_section'] = df['event_params_json'].apply(lambda x: extract_param(x, 'faq_section'))
    df['faq_id'] = df['event_params_json'].apply(lambda x: extract_param(x, 'faq_id'))
    return df

# New date handling utilities
def get_filtered_date_range(df, context):
    """Helper to consistently handle date filtering across views
    
    Args:
        df (pd.DataFrame): DataFrame to filter
        context (dict): Context dictionary containing date range info
        
    Returns:
        pd.DataFrame: Filtered DataFrame
        dict: Date range information (start_date, end_date, available_days)
    """
    if 'date' not in df.columns:
        return df, {
            'start_date': None,
            'end_date': None,
            'available_days': None
        }
        
    df = df.copy()
    
    # Ensure date column is datetime
    try:
        df['date'] = pd.to_datetime(df['date'])
    except (ValueError, TypeError):
        return df, {
            'start_date': None,
            'end_date': None,
            'available_days': None
        }
    
    # Get date range from context
    selected_date_range = context.get('selected_date_range', {})
    if not isinstance(selected_date_range, dict):
        selected_date_range = {}
        
    start_date = selected_date_range.get('start')
    end_date = selected_date_range.get('end')
    
    # If no dates in context, use data's min/max
    if not start_date:
        start_date = df['date'].min()
    if not end_date:
        end_date = df['date'].max()
        
    # Convert to datetime if needed
    try:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
    except (ValueError, TypeError):
        return df, {
            'start_date': None,
            'end_date': None,
            'available_days': None
        }
    
    # Validate date range
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    
    # Filter DataFrame
    df = df[
        (df['date'] >= start_date) &
        (df['date'] <= end_date)
    ]
    
    available_days = (end_date - start_date).days + 1
    
    return df, {
        'start_date': start_date,
        'end_date': end_date,
        'available_days': available_days
    }

def get_comparison_periods(df, days_to_compare, end_date=None):
    """Calculate current and previous period data ranges
    
    Args:
        df (pd.DataFrame): DataFrame with 'date' column
        days_to_compare (int): Number of days to compare
        end_date (datetime, optional): End date for current period. Defaults to max date.
        
    Returns:
        tuple: (current_period_df, previous_period_df, period_info)
    """
    if 'date' not in df.columns or df.empty:
        return df.iloc[0:0], df.iloc[0:0], {}  # Return empty DataFrames and empty info
        
    try:
        df['date'] = pd.to_datetime(df['date'])
    except (ValueError, TypeError):
        return df.iloc[0:0], df.iloc[0:0], {}
    
    if end_date is None:
        end_date = df['date'].max()
    else:
        try:
            end_date = pd.to_datetime(end_date)
        except (ValueError, TypeError):
            return df.iloc[0:0], df.iloc[0:0], {}
    
    # Calculate period boundaries
    current_period_start = end_date - pd.Timedelta(days=days_to_compare - 1)
    previous_period_end = current_period_start - pd.Timedelta(days=1)
    previous_period_start = previous_period_end - pd.Timedelta(days=days_to_compare - 1)
    
    # Ensure we don't try to look beyond available data
    min_date = df['date'].min()
    if previous_period_start < min_date:
        # Calculate how many days we can actually compare
        total_available_days = (end_date - min_date).days + 1
        max_comparison_days = total_available_days // 2  # Need equal periods
        
        if max_comparison_days < 7:  # If we can't even do a 7-day comparison
            return df.iloc[0:0], df.iloc[0:0], {
                'current_start': current_period_start,
                'current_end': end_date,
                'previous_start': previous_period_start,
                'previous_end': previous_period_end,
                'error': 'insufficient_history',
                'max_comparison_days': max_comparison_days,
                'total_available_days': total_available_days
            }
        
        # Recalculate periods using maximum possible days
        days_to_compare = max_comparison_days
        current_period_start = end_date - pd.Timedelta(days=days_to_compare - 1)
        previous_period_end = current_period_start - pd.Timedelta(days=1)
        previous_period_start = previous_period_end - pd.Timedelta(days=days_to_compare - 1)
    
    # Get the data for each period
    current_period = df[
        (df['date'] >= current_period_start) & 
        (df['date'] <= end_date)
    ].copy()
    
    previous_period = df[
        (df['date'] >= previous_period_start) &
        (df['date'] <= previous_period_end)
    ].copy()
    
    # Verify we have data for both periods
    current_days = len(current_period['date'].unique())
    previous_days = len(previous_period['date'].unique())
    
    # Allow for some missing days (up to 20% missing)
    expected_days = days_to_compare
    min_required_days = int(expected_days * 0.8)
    
    if current_days < min_required_days or previous_days < min_required_days:
        return df.iloc[0:0], df.iloc[0:0], {
            'current_start': current_period_start,
            'current_end': end_date,
            'previous_start': previous_period_start,
            'previous_end': previous_period_end,
            'error': 'incomplete_data',
            'current_days': current_days,
            'previous_days': previous_days,
            'expected_days': expected_days,
            'min_required_days': min_required_days
        }
    
    period_info = {
        'current_start': current_period_start,
        'current_end': end_date,
        'previous_start': previous_period_start,
        'previous_end': previous_period_end,
        'current_days': current_days,
        'previous_days': previous_days,
        'days_compared': days_to_compare
    }
    
    return current_period, previous_period, period_info
