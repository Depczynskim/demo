import os
import glob
import json
from datetime import datetime
from typing import Any, Dict

import pandas as pd
import openai
from dotenv import load_dotenv
from pathlib import Path

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from logger import get_logger
from copilot.utils.openai_client import get_openai_client

# Load environment variables from .env file
load_dotenv()

logger = get_logger(__name__)

# Output directory path (copilot/summaries)
SUMMARY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summaries'))
DATA_DIR = "data_repo/google_ads/google_ads_final"
SESSIONS_PATH = Path('data_repo/ga4/sessions_latest.parquet')


def find_latest_parquet(data_dir):
    """Find the latest parquet file in the nested report_month directories."""
    month_dirs = [os.path.join(data_dir, d) for d in os.listdir(data_dir) if d.startswith("report_month=")]
    latest_file = None
    latest_mtime = 0
    for month_dir in month_dirs:
        parquet_files = glob.glob(os.path.join(month_dir, "*.parquet"))
        for f in parquet_files:
            mtime = os.path.getmtime(f)
            if mtime > latest_mtime:
                latest_file = f
                latest_mtime = mtime
    return latest_file


def compute_metrics(df, date_window: int | None = None, sessions_df: pd.DataFrame | None = None):
    """Compute key Google Ads metrics from the dataframe."""
    metrics = {}
    metrics['total_impressions'] = int(df['impressions'].sum())
    metrics['total_clicks'] = int(df['clicks'].sum())
    metrics['total_cost'] = float(df['cost'].sum())
    metrics['total_conversions'] = int(df['conversions'].sum())
    metrics['total_conversion_value'] = float(df['conversions_value'].sum() if 'conversions_value' in df.columns else 0)

    metrics['ctr'] = (
        metrics['total_clicks'] / metrics['total_impressions'] * 100 if metrics['total_impressions'] > 0 else 0
    )
    metrics['avg_cpc'] = (
        metrics['total_cost'] / metrics['total_clicks'] if metrics['total_clicks'] > 0 else 0
    )
    metrics['cpa'] = (
        metrics['total_cost'] / metrics['total_conversions'] if metrics['total_conversions'] > 0 else 0
    )
    metrics['roas'] = (
        metrics['total_conversion_value'] / metrics['total_cost'] if metrics['total_cost'] > 0 else 0
    )

    # ------------------------------------------------------------------
    # Level-5 session attribution (global) – based on GA4 sessions parquet
    # ------------------------------------------------------------------

    if sessions_df is not None and not sessions_df.empty:
        s_df = sessions_df.copy()

        # Filter window
        if date_window is not None and 'date' in s_df.columns:
            cutoff_dt = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=date_window)
            s_df = s_df[pd.to_datetime(s_df['date'], errors='coerce') >= cutoff_dt]

        # Only Ads traffic (cpc medium)
        ads_l5 = s_df[(s_df['traffic_medium'] == 'cpc') & (s_df['engagement_level'] == 5)]
        l5_count = len(ads_l5)
        metrics['l5_sessions'] = l5_count
        metrics['cost_per_l5'] = (
            metrics['total_cost'] / l5_count if l5_count > 0 else None
        )
    else:
        metrics['l5_sessions'] = None
        metrics['cost_per_l5'] = None

    # ------------------------------------------------------------
    # Per-campaign deep dive for the top 5 campaigns by clicks
    # ------------------------------------------------------------

    campaign_breakdowns: dict[str, Any] = {}
    if 'campaign_name' in df.columns:
        # Aggregate stats for **all** campaigns
        all_campaigns = df['campaign_name'].dropna().unique()
        clicks_by_campaign = df.groupby('campaign_name')['clicks'].sum().sort_values(ascending=False)
        for cname in all_campaigns:
            c_df = df[df['campaign_name'] == cname]
            c_metrics: dict[str, Any] = {}
            c_metrics['impressions'] = int(c_df['impressions'].sum())
            c_metrics['clicks'] = int(c_df['clicks'].sum())
            c_metrics['cost'] = float(c_df['cost'].sum())
            c_metrics['conversions'] = int(c_df['conversions'].sum())
            conv_value = float(c_df['conversions_value'].sum()) if 'conversions_value' in c_df.columns else 0
            c_metrics['conversion_value'] = conv_value
            c_metrics['ctr'] = (
                c_metrics['clicks'] / c_metrics['impressions'] * 100 if c_metrics['impressions'] > 0 else 0
            )
            c_metrics['cpa'] = (
                c_metrics['cost'] / c_metrics['conversions'] if c_metrics['conversions'] > 0 else 0
            )
            c_metrics['roas'] = (
                conv_value / c_metrics['cost'] if c_metrics['cost'] > 0 else 0
            )

            # Average CPC for the campaign
            c_metrics['avg_cpc'] = (
                c_metrics['cost'] / c_metrics['clicks'] if c_metrics['clicks'] > 0 else 0
            )

            # Top keywords within campaign
            if 'keyword_text' in c_df.columns:
                c_metrics['top_keywords'] = (
                    c_df.groupby('keyword_text')['clicks'].sum().nlargest(5).to_dict()
                )

            # Clicks by day-of-week
            if 'date' in c_df.columns:
                c_metrics['clicks_by_day'] = (
                    pd.to_datetime(c_df['date']).dt.day_name().value_counts().head(7).to_dict()
                )

            campaign_breakdowns[cname] = c_metrics

    metrics['campaign_breakdowns'] = campaign_breakdowns if campaign_breakdowns else None

    # Top 5 campaigns by clicks
    if 'campaign_name' in df.columns:
        top_campaigns = (
            df.groupby('campaign_name')['clicks'].sum().nlargest(5)
        )
        metrics['top_campaigns'] = top_campaigns.to_dict()
    else:
        metrics['top_campaigns'] = None

    # Estimate per-campaign L5 sessions proportionally to clicks (fallback)
    if metrics.get('l5_sessions') and metrics['l5_sessions'] > 0 and metrics.get('campaign_breakdowns'):
        total_clicks = metrics['total_clicks']
        if total_clicks > 0:
            for cname, cm in metrics['campaign_breakdowns'].items():
                click_share = cm['clicks'] / total_clicks if total_clicks else 0
                est_l5 = int(round(metrics['l5_sessions'] * click_share))
                cm['est_l5_sessions'] = est_l5
                cm['cost_per_est_l5'] = cm['cost'] / est_l5 if est_l5 else None

    # Click distribution by day-of-week (if date available)
    if 'date' in df.columns:
        dow_counts = (
            pd.to_datetime(df['date']).dt.day_name().value_counts().head(7)
        )
        metrics['clicks_by_day'] = dow_counts.to_dict()
    else:
        metrics['clicks_by_day'] = None

    metrics['window_days'] = date_window

    # ------------------------------------------------------------------
    # Previous-window deltas (days → 2×days)
    # ------------------------------------------------------------------
    prev_df = pd.DataFrame()
    if 'date' in df.columns:
        date_all_ns = pd.to_datetime(df['date'], errors='coerce').view('int64')
        max_ts = pd.to_datetime(df['date'], errors='coerce').max()
        if pd.notna(max_ts):
            prev_upper_ns = (max_ts - pd.Timedelta(days=date_window)).value
            prev_lower_ns = (max_ts - pd.Timedelta(days=2 * date_window)).value
            prev_mask = (date_all_ns >= prev_lower_ns) & (date_all_ns < prev_upper_ns)
            prev_df = df[prev_mask]

    if not prev_df.empty:
        prev_metrics = compute_metrics(prev_df, date_window=date_window, sessions_df=None)

        def _d(cur, prev):
            try:
                return cur - prev
            except Exception:
                return None

        delta_map = {
            'total_impressions': 'impressions_delta',
            'total_clicks': 'clicks_delta',
            'total_cost': 'cost_delta',
            'total_conversions': 'conversions_delta',
            'total_conversion_value': 'conversion_value_delta',
            'l5_sessions': 'l5_sessions_delta',
            'cost_per_l5': 'cost_per_l5_delta',
        }
        for k_src, k_out in delta_map.items():
            metrics[k_out] = _d(metrics.get(k_src), prev_metrics.get(k_src))
    else:
        for suffix in ['impressions', 'clicks', 'cost', 'conversions', 'conversion_value']:
            metrics[f"{suffix}_delta"] = None
        for suffix in ['l5_sessions', 'cost_per_l5']:
            metrics[f"{suffix}_delta"] = None

    return metrics


def write_markdown_summary(metrics: dict[str, Any], output_path: str, date_range=None, narrative: str | None = None):
    """Write a Markdown summary file with front-matter metadata and metrics."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    with open(output_path, 'w') as f:
        f.write(f"---\n")
        f.write(f"source: google_ads\n")
        if date_range:
            f.write(f"date_range: {date_range}\n")
        f.write(f"generated_at: {now}\n")
        if metrics.get('window_days'):
            f.write(f"window_days: {metrics['window_days']}\n")
        f.write(f"---\n\n")
        f.write(f"# Google Ads Performance Summary\n\n")
        f.write(f"- **Total Impressions:** {metrics['total_impressions']:,}\n")
        f.write(f"- **Total Clicks:** {metrics['total_clicks']:,}\n")
        f.write(f"- **Total Cost:** £{metrics['total_cost']:.2f}\n")
        f.write(f"- **Total Conversions:** {metrics['total_conversions']:,}\n")
        f.write(f"- **Total Conversion Value:** £{metrics['total_conversion_value']:.2f}\n")
        f.write(f"- **CTR:** {metrics['ctr']:.2f}%\n")
        f.write(f"- **Average CPC:** £{metrics['avg_cpc']:.2f}\n")
        f.write(f"- **CPA:** £{metrics['cpa']:.2f}\n")
        f.write(f"- **ROAS:** {metrics['roas']:.2f}\n")

        if metrics.get('top_campaigns'):
            f.write("\n## Top 5 Campaigns by Clicks\n")
            for name, clicks in metrics['top_campaigns'].items():
                f.write(f"- {name}: {clicks:,} clicks\n")

        # ---------------------------------------------
        # Per-campaign breakdown sections
        # ---------------------------------------------
        if metrics.get('campaign_breakdowns'):
            # Order spotlight sections by highest spend (cost) for business relevance
            sorted_names = sorted(
                metrics['campaign_breakdowns'].keys(),
                key=lambda n: metrics['campaign_breakdowns'][n]['cost'],
                reverse=True,
            )
            max_to_show = 15  # avoid extremely long markdown
            for idx, cname in enumerate(sorted_names):
                if idx >= max_to_show:
                    f.write(f"\n_...{len(sorted_names)-max_to_show} more campaigns omitted for brevity..._\n")
                    break
                cm = metrics['campaign_breakdowns'][cname]
                f.write(f"\n---\n\n## Campaign Spotlight: {cname}\n")
                f.write(f"- **Impressions:** {cm['impressions']:,}\n")
                f.write(f"- **Clicks:** {cm['clicks']:,}\n")
                f.write(f"- **Cost:** £{cm['cost']:.2f}\n")
                f.write(f"- **Conversions:** {cm['conversions']:,}\n")
                f.write(f"- **ROAS:** {cm['roas']:.2f}\n")

                # Additional KPIs
                f.write(f"- **CTR:** {cm['ctr']:.2f}%\n")
                f.write(f"- **Avg CPC:** £{cm['avg_cpc']:.2f}\n")
                f.write(f"- **CPA:** £{cm['cpa']:.2f}\n")
                f.write(f"- **Conversion Value:** £{cm['conversion_value']:.2f}\n")

                if cm.get('top_keywords'):
                    f.write("\n### Top Keywords\n")
                    for kw, clk in cm['top_keywords'].items():
                        f.write(f"- {kw}: {clk:,} clicks\n")
                if cm.get('clicks_by_day'):
                    f.write("\n### Clicks by Day of Week\n")
                    for d, cval in cm['clicks_by_day'].items():
                        f.write(f"- {d}: {cval:,} clicks\n")

        if narrative:
            f.write("\n---\n\n## Narrative Summary\n\n" + narrative.strip() + "\n")


def generate_narrative(metrics: dict[str, Any]) -> str | None:
    if not OPENAI_ENABLED:
        logger.warning("OPENAI_API_KEY not set – skipping narrative generation.")
        return None
    prompt = (
        "Summarise in 2-3 sentences the key insights and recommendations from these Google Ads KPIs. "
        "Focus on performance trends and optimisation opportunities.\n"
        f"Metrics JSON: {json.dumps(metrics, default=str)}"
    )
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=120,
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Narrative generation failed: {e}")
        return None


def write_json_summary(metrics: dict[str, Any], output_path: str):
    """Dump metrics dict to an indented JSON file."""
    import json, pathlib
    pathlib.Path(output_path).write_text(json.dumps(metrics, default=str, indent=2))


def generate_summaries(df):
    # Load sessions parquet if available
    if SESSIONS_PATH.exists():
        sessions_df = pd.read_parquet(SESSIONS_PATH)
    else:
        sessions_df = None

    # Switch to 30-day short-term window so that Ads summaries match the
    # unified 30/90/365-day cadence used across Copilot components.
    windows = [30, 90, 365]
    for days in windows:
        df_filtered = df.copy()
        if 'date' in df_filtered.columns:
            date_ns = pd.to_datetime(df_filtered['date'], errors='coerce').view('int64')
            cutoff_ns_today = (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).value
            df_filtered = df_filtered[date_ns >= cutoff_ns_today]

            # Fallback: if empty, take the last <days> relative to the most recent date present
            if df_filtered.empty and not pd.isna(date_ns).all():
                max_ts = pd.to_datetime(df_filtered['date'], errors='coerce').max()
                if pd.notna(max_ts):
                    cutoff_ns_recent = (max_ts - pd.Timedelta(days=days)).value
                    df_filtered = df[date_ns >= cutoff_ns_recent]
        metrics = compute_metrics(df_filtered, date_window=days, sessions_df=sessions_df)
        narrative = generate_narrative(metrics)
        suffix = f"{days}d"
        output_path = os.path.join(SUMMARY_DIR, f"google_ads_summary_{suffix}.md")
        write_markdown_summary(metrics, output_path, date_range=metrics.get('date_range'), narrative=narrative)

        json_path = os.path.join(SUMMARY_DIR, f"google_ads_summary_{suffix}.json")
        write_json_summary(metrics, json_path)

        print(f"Summary written to {output_path}")


def main():
    os.makedirs(SUMMARY_DIR, exist_ok=True)
    latest_file = find_latest_parquet(DATA_DIR)
    if not latest_file:
        print("No Google Ads parquet file found.")
        return
    df = pd.read_parquet(latest_file)
    generate_summaries(df)


if __name__ == "__main__":
    main()

def summarize_google_ads_data(data: Dict[str, Any], model: str | None = None) -> str:
    """Return a natural language summary of the Google Ads data."""
    # Initialize OpenAI client only when needed
    client = get_openai_client()
    
    model_name = model or os.getenv("COPILOT_COMPLETION_MODEL", "gpt-3.5-turbo-0125")
    
    # Prepare the prompt
    prompt = (
        "You are a data analyst summarizing Google Ads performance data. "
        "Provide a clear, concise summary of the key metrics and insights. "
        "Focus on the most important changes and patterns in ad performance. "
        "Use natural language and avoid technical jargon.\n\n"
        "Here is the Google Ads data to summarize:\n"
        f"{data}"
    )
    
    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": "Please summarize this Google Ads data."},
            ],
            temperature=0.3,
            max_tokens=500,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error generating Google Ads summary: {str(e)}" 