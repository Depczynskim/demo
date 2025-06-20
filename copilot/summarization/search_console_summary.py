import os
import glob
import json
from datetime import datetime
from typing import Any
from pathlib import Path
import textwrap

import pandas as pd
import openai
from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from logger import get_logger

# Load environment variables from .env file
load_dotenv()

# Create a client instance
try:
    client = openai.OpenAI()
    # A quick check to see if the key is actually available for use.
    # If not, client.api_key will be None.
    OPENAI_ENABLED = client.api_key is not None
except openai.OpenAIError:
    OPENAI_ENABLED = False

logger = get_logger(__name__)

# Output directory path (copilot/summaries)
SUMMARY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summaries'))
DATA_DIR = Path(__file__).resolve().parents[2] / "data_repo"

# Static minimal ISO-3 → full country name map (extend as needed)
ISO3_MAP = {
    'USA': 'United States',
    'GBR': 'United Kingdom',
    'IRL': 'Ireland',
    'AUS': 'Australia',
    'ARG': 'Argentina',
    'ESP': 'Spain',
    'ARE': 'United Arab Emirates',
    'BRA': 'Brazil',
    'CAN': 'Canada',
    'DEU': 'Germany',
    'FRA': 'France',
    'NLD': 'Netherlands',
    'CUW': 'Curaçao',
    # add more if needed
}


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


def compute_metrics(current_df: pd.DataFrame, previous_df: pd.DataFrame, date_window: int) -> dict[str, Any]:
    """Aggregate KPI metrics for current window and deltas vs previous window of equal length."""
    metrics: dict[str, Any] = {}

    if current_df.empty:
        metrics.update({
            'is_empty': True,
            'clicks': 0,
            'impressions': 0,
            'avg_position': 0,
            'ctr': 0,
            'clicks_delta': 0,
            'impressions_delta': 0,
            'avg_position_delta': 0,
            'ctr_delta': 0,
            'date_range': "N/A",
            'period_days': 0,
            'window_days': date_window,
        })
        return metrics

    # Ensure date column typed and country_name present
    current_df = current_df.copy()
    previous_df = previous_df.copy()
    for d in (current_df, previous_df):
        if 'date' in d.columns:
            d['date'] = pd.to_datetime(d['date'])
        if 'country' in d.columns and 'country_name' not in d.columns:
            d['country_name'] = d['country'].apply(iso3_to_country)

    def agg_stats(d):
        clicks = int(d['clicks'].sum()) if 'clicks' in d.columns else 0
        impressions = int(d['impressions'].sum()) if 'impressions' in d.columns else 0
        avg_position = float(d['position'].mean()) if 'position' in d.columns and not d.empty else None
        ctr = (clicks / impressions * 100) if impressions > 0 else 0
        return clicks, impressions, avg_position, ctr

    c_clicks, c_impr, c_pos, c_ctr = agg_stats(current_df)
    p_clicks, p_impr, p_pos, p_ctr = agg_stats(previous_df) if not previous_df.empty else (0, 0, None, 0)

    metrics.update({
        'clicks': c_clicks,
        'impressions': c_impr,
        'avg_position': c_pos if c_pos is not None else 0,
        'ctr': c_ctr,
        'clicks_delta': c_clicks - p_clicks,
        'impressions_delta': c_impr - p_impr,
        'avg_position_delta': (c_pos - p_pos) if c_pos is not None and p_pos is not None else 0,
        'ctr_delta': c_ctr - p_ctr,
        'date_range': f"{current_df['date'].min().strftime('%Y-%m-%d')} to {current_df['date'].max().strftime('%Y-%m-%d')}",
        'period_days': date_window,
        'window_days': date_window,
    })

    # Device-wise clicks for current window
    if 'device' in current_df.columns:
        metrics['clicks_by_device'] = current_df.groupby('device')['clicks'].sum().to_dict()
    else:
        metrics['clicks_by_device'] = None

    # ------------------------------------------------------------
    # Country-level breakdowns (top 5 by clicks in current period)
    # ------------------------------------------------------------
    country_breakdowns: dict[str, Any] = {}
    if 'country_name' in current_df.columns and not current_df.empty:
        top_countries_series = current_df.groupby('country_name')['clicks'].sum().nlargest(5)
        for ctry in top_countries_series.index:
            cur_ctry_df = current_df[current_df['country_name'] == ctry]
            prev_ctry_df = previous_df[previous_df['country_name'] == ctry]

            def _stat_block(dframe: pd.DataFrame):
                cks = int(dframe['clicks'].sum())
                impr = int(dframe['impressions'].sum())
                pos = float(dframe['position'].mean()) if not dframe.empty else None
                ctr_val = (cks / impr * 100) if impr > 0 else 0
                return cks, impr, pos, ctr_val

            cur_clicks, cur_impr, cur_pos, cur_ctr = _stat_block(cur_ctry_df)
            prev_clicks, prev_impr, prev_pos, prev_ctr = _stat_block(prev_ctry_df)

            c_metrics = {
                'clicks': cur_clicks,
                'impressions': cur_impr,
                'avg_position': cur_pos,
                'ctr': cur_ctr,
                'clicks_delta': cur_clicks - prev_clicks,
                'impressions_delta': cur_impr - prev_impr,
                'avg_position_delta': (cur_pos - prev_pos) if cur_pos is not None and prev_pos is not None else None,
                'ctr_delta': cur_ctr - prev_ctr,
            }

            # Skip countries with zero clicks to avoid noise
            if cur_clicks == 0:
                continue

            # Country top queries/pages (current period)
            if 'query' in cur_ctry_df.columns:
                q_series = cur_ctry_df.groupby('query')['clicks'].sum()
                q_filtered = q_series[q_series > 0].nlargest(5)
                c_metrics['top_queries'] = q_filtered.to_dict() if not q_filtered.empty else None
            if 'page' in cur_ctry_df.columns:
                p_series = cur_ctry_df.groupby('page')['clicks'].sum()
                p_filtered = p_series[p_series > 0].nlargest(5)
                c_metrics['top_pages'] = p_filtered.to_dict() if not p_filtered.empty else None

            country_breakdowns[ctry] = c_metrics

    metrics['country_breakdowns'] = country_breakdowns if country_breakdowns else None

    # Remove global top-query/page/country lists (now in breakdowns)
    metrics.pop('top_countries', None)
    metrics.pop('top_queries', None)
    metrics.pop('top_pages', None)

    return metrics


def write_markdown_summary(metrics: dict[str, Any], output_path: str, narrative: str | None = None):
    """Write a Markdown summary file with front-matter metadata and metrics."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    with open(output_path, 'w') as f:
        f.write(f"---\n")
        f.write(f"source: search_console\n")
        f.write(f"date_range: {metrics['date_range']}\n")
        f.write(f"generated_at: {now}\n")
        if metrics.get('window_days'):
            f.write(f"window_days: {metrics['window_days']}\n")
        f.write(f"---\n\n")
        f.write(f"# Search Console Performance Summary\n\n")
        f.write(f"- **Total Clicks:** {metrics['clicks']:,} (Δ {metrics['clicks_delta']:+,})\n")
        f.write(f"- **Total Impressions:** {metrics['impressions']:,} (Δ {metrics['impressions_delta']:+,})\n")
        f.write(f"- **Average Position:** {metrics['avg_position']:.2f} (Δ {metrics['avg_position_delta']:+.2f})\n")
        f.write(f"- **CTR:** {metrics['ctr']:.2f}% (Δ {metrics['ctr_delta']:+.2f}%)\n")
        f.write(f"- **Period:** {metrics['date_range']} ({metrics['period_days']} days)\n")

        # Early exit if dataframe truly empty
        if metrics.get('is_empty'):
            f.write("\n_No Search Console data available for this period._\n")
            return

        # Device breakdown (optional)
        if metrics.get('clicks_by_device'):
            f.write("\n## Clicks by Device\n")
            for dev, clicks in metrics['clicks_by_device'].items():
                f.write(f"- {dev}: {clicks:,} clicks\n")

        # --------------------------------------------------------
        # Country spotlight sections
        # --------------------------------------------------------
        if metrics.get('country_breakdowns'):
            for ctry, cm in metrics['country_breakdowns'].items():
                f.write(f"\n---\n\n## Country Spotlight: {ctry}\n")
                f.write(f"- **Clicks:** {cm['clicks']:,} (Δ {cm['clicks_delta']:+,})\n")
                f.write(f"- **Impressions:** {cm['impressions']:,} (Δ {cm['impressions_delta']:+,})\n")
                if cm['avg_position'] is not None:
                    pos_delta_str = (
                        f"{cm['avg_position_delta']:+.2f}" if cm['avg_position_delta'] is not None else "N/A"
                    )
                    f.write(f"- **Avg Position:** {cm['avg_position']:.2f} (Δ {pos_delta_str})\n")
                f.write(f"- **CTR:** {cm['ctr']:.2f}% (Δ {cm['ctr_delta']:+.2f}%)\n")

                if cm.get('top_queries'):
                    f.write("\n### Top Queries\n")
                    for q, c in cm['top_queries'].items():
                        f.write(f"- {q}: {c:,} clicks\n")
                if cm.get('top_pages'):
                    f.write("\n### Top Pages\n")
                    for p, c in cm['top_pages'].items():
                        short_p = p.split('?')[0]
                        if len(short_p) > 120:
                            short_p = short_p[:117] + '...'
                        f.write(f"- {short_p}: {c:,} clicks\n")

        if narrative:
            f.write("\n---\n\n## Narrative Summary\n\n" + narrative.strip() + "\n")


def generate_narrative(metrics: dict[str, Any]) -> str | None:
    if metrics.get('is_empty'):
        return None
    if not OPENAI_ENABLED:
        logger.warning("OPENAI_API_KEY not set – skipping narrative generation.")
        return None
    prompt = (
        "You are an SEO analytics assistant. Write a concise executive summary (≈3 sentences) focusing on: "
        "1) notable overall trends, 2) the biggest country-level differences (click growth, position shifts), "
        "and 3) new high-performing queries or pages.\n"
        f"Metrics JSON: {json.dumps(metrics, default=str)}"
    )
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=180,
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Narrative generation failed: {e}")
        return None


def generate_summaries(df):
    windows = [30, 90, 365]
    for days in windows:
        if 'date' not in df.columns:
            continue
        all_dates = pd.to_datetime(df['date'], errors='coerce')
        max_ts = all_dates.max()
        current_start = max_ts - pd.Timedelta(days=days-1)
        current_df = df[all_dates >= current_start].copy()

        # Previous period window
        prev_end = current_start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=days-1)
        prev_df = df[(all_dates >= prev_start) & (all_dates <= prev_end)].copy()

        metrics = compute_metrics(current_df, prev_df, date_window=days)
        narrative = None if metrics.get('is_empty') else (None if metrics['clicks'] == 0 else generate_narrative(metrics))
        output_path = os.path.join(SUMMARY_DIR, f"search_console_summary_{days}d.md")
        write_markdown_summary(metrics, output_path, narrative=narrative)

        # JSON too
        json_path = os.path.join(SUMMARY_DIR, f"search_console_summary_{days}d.json")
        write_json_summary(metrics, json_path)

        print(f"Summary written to {output_path}")


def main():
    os.makedirs(SUMMARY_DIR, exist_ok=True)
    df = load_all_parquets(DATA_DIR)
    if df.empty:
        print("No Search Console parquet files found.")
        return
    generate_summaries(df)


# ---------------------------------------------------------------------------
# JSON helper
# ---------------------------------------------------------------------------


def write_json_summary(metrics: dict[str, Any], output_path: str):
    import json, pathlib
    pathlib.Path(output_path).write_text(json.dumps(metrics, default=str, indent=2))


# ---------------------------------------------------------------------------
# Helpers: load & enrich dataframe
# ---------------------------------------------------------------------------


def load_all_parquets(base_dir: str) -> pd.DataFrame:
    """Concatenate all monthly parquet files under the Search-Console directory."""
    paths = glob.glob(os.path.join(base_dir, "report_month=*", "*.parquet"))
    if not paths:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)


def iso3_to_country(alpha3: str) -> str:
    """Convert ISO-3 code to full country name using static map, pycountry fallback, or code as-is."""
    if not isinstance(alpha3, str):
        return str(alpha3)
    code = alpha3.upper()
    if code in ISO3_MAP:
        return ISO3_MAP[code]
    try:
        import pycountry
        c = pycountry.countries.get(alpha_3=code)
        return c.name if c else code
    except Exception:
        return code


if __name__ == "__main__":
    main() 