import os
import glob
import json
from datetime import datetime
from typing import Any, List
from pathlib import Path

import pandas as pd
import openai
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment & logging utils (reuse lightweight logger from utils)
# ---------------------------------------------------------------------------

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from logger import get_logger  # noqa: E402

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
logger = get_logger(__name__)

# Directory where the GA4 / Ads / SC summaries live – keep combined ones alongside
SUMMARY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summaries'))
GA4_JSON_PATTERN = "ga4_summary_{suffix}.json"
ADS_JSON_PATTERN = "google_ads_summary_{suffix}.json"
SC_JSON_PATTERN = "search_console_summary_{suffix}.json"
# ---------------------------------------------------------------------------


def build_metrics_from_json(window_days: int) -> dict[str, Any]:
    suffix = f"{window_days}d"

    def _load(path):
        import json as _j
        return _j.loads(Path(path).read_text()) if Path(path).exists() else {}

    ga4_json = _load(Path(SUMMARY_DIR) / GA4_JSON_PATTERN.format(suffix=suffix))
    ads_json = _load(Path(SUMMARY_DIR) / ADS_JSON_PATTERN.format(suffix=suffix))
    sc_json = _load(Path(SUMMARY_DIR) / SC_JSON_PATTERN.format(suffix=suffix))

    tot_sessions = ga4_json.get("sessions") or 0
    tot_conv = ga4_json.get("conversions") or 0

    ads_impr = ads_json.get("total_impressions") or 0
    ads_clicks = ads_json.get("total_clicks") or 0
    ads_cost = ads_json.get("total_cost") or 0.0
    ads_l5 = ads_json.get("l5_sessions") or 0
    cost_per_l5 = ads_json.get("cost_per_l5")

    sc_impr = sc_json.get("total_impressions") or 0
    sc_clicks = sc_json.get("total_clicks") or 0

    def _pct(n, d):
        return (n / d * 100) if d else 0.0

    date_range_val = ga4_json.get("date_range") or f"last {window_days} days"
    ads_cpc = (ads_cost / ads_clicks) if ads_clicks else None
    metrics = {
        "window_days": window_days,
        "date_range": date_range_val,
        "sessions": tot_sessions,
        "conversions": tot_conv,
        "ads_impressions": ads_impr,
        "ads_clicks": ads_clicks,
        "ads_cost": ads_cost,
        "ads_ctr": _pct(ads_clicks, ads_impr),
        "ads_l5_sessions": ads_l5,
        "ads_cost_per_l5": cost_per_l5,
        "search_impressions": sc_impr,
        "search_clicks": sc_clicks,
        "search_ctr": _pct(sc_clicks, sc_impr),
        "conversion_rate_pct": _pct(tot_conv, tot_sessions),
        "level_counts": ga4_json.get("level_counts"),
        "signal_lifts": ga4_json.get("signal_lifts"),
    }

    # ------------------------------------------------------------------
    # Copy any *_delta fields from the individual source JSONs so the
    # prompt can quote proper period-over-period changes.
    # ------------------------------------------------------------------
    for src_json in (ga4_json, ads_json, sc_json):
        for k, v in src_json.items():
            if k.endswith("_delta") and k not in metrics:
                metrics[k] = v if v is not None else 0

    return metrics


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def safe_sum(series: pd.Series) -> float:
    return float(series.dropna().sum()) if not series.empty else 0.0


def compute_correlations(df: pd.DataFrame) -> dict[str, Any]:
    """Return a set of Pearson correlations for key funnel relationships."""
    def _corr(col_a: str, col_b: str):
        if col_a not in df.columns or col_b not in df.columns:
            return None
        sub = df[[col_a, col_b]].dropna()
        if len(sub) < 7:
            return None  # not enough data points
        return float(sub[col_a].corr(sub[col_b]))

    return {
        "ads_clicks_vs_sessions": _corr("clicks_ads", "sessions"),
        "search_clicks_vs_sessions": _corr("clicks_sc", "sessions"),
        "ads_cost_vs_conversions": _corr("cost_ads", "conversions"),
    }


def compute_metrics(cur_df: pd.DataFrame, prev_df: pd.DataFrame, window_days: int) -> dict[str, Any]:
    metrics: dict[str, Any] = {}

    # Totals for current window ------------------------------------------------------------
    tot_sessions = safe_sum(cur_df.get("sessions", pd.Series(dtype=float)))
    tot_conv = safe_sum(cur_df.get("conversions", pd.Series(dtype=float)))

    tot_ads_impr = safe_sum(cur_df.get("impressions_ads", pd.Series(dtype=float)))
    tot_ads_clicks = safe_sum(cur_df.get("clicks_ads", pd.Series(dtype=float)))
    tot_ads_conv = safe_sum(cur_df.get("conversions_ads", pd.Series(dtype=float)))
    tot_ads_cost = safe_sum(cur_df.get("cost_ads", pd.Series(dtype=float)))

    tot_sc_impr = safe_sum(cur_df.get("impressions_sc", pd.Series(dtype=float)))
    tot_sc_clicks = safe_sum(cur_df.get("clicks_sc", pd.Series(dtype=float)))

    # Derived metrics ----------------------------------------------------------------------
    def _pct(num, den):
        return (num / den * 100) if den else 0.0

    ads_ctr = _pct(tot_ads_clicks, tot_ads_impr)
    sc_ctr = _pct(tot_sc_clicks, tot_sc_impr)

    click_to_session = _pct(tot_ads_clicks + tot_sc_clicks, tot_sessions)  # expressed as %
    conv_rate = _pct(tot_conv, tot_sessions)
    cpc_ads = (tot_ads_cost / tot_ads_clicks) if tot_ads_clicks else 0.0
    cpa_ads = (tot_ads_cost / tot_ads_conv) if tot_ads_conv else 0.0
    cost_per_session = (tot_ads_cost / tot_sessions) if tot_sessions else 0.0

    # Ensure we have datetime objects for date_range calculation
    if not cur_df.empty:
        _dates = pd.to_datetime(cur_df["date"], errors="coerce")
        min_dt = _dates.min()
        max_dt = _dates.max()
        date_range_str = f"{min_dt.strftime('%Y-%m-%d')} to {max_dt.strftime('%Y-%m-%d')}" if pd.notna(min_dt) and pd.notna(max_dt) else "N/A"
    else:
        date_range_str = "N/A"

    # Populate metric dict -----------------------------------------------------------------
    metrics.update(
        {
            "window_days": window_days,
            "date_range": date_range_str,
            # Totals
            "sessions": tot_sessions,
            "conversions": tot_conv,
            "ads_impressions": tot_ads_impr,
            "ads_clicks": tot_ads_clicks,
            "ads_conversions": tot_ads_conv,
            "ads_cost": tot_ads_cost,
            "search_impressions": tot_sc_impr,
            "search_clicks": tot_sc_clicks,
            # Derived
            "ads_ctr": ads_ctr,
            "search_ctr": sc_ctr,
            "click_to_session_pct": click_to_session,
            "conversion_rate_pct": conv_rate,
            "ads_cpc": cpc_ads,
            "ads_cpa": cpa_ads,
            "cost_per_session": cost_per_session,
        }
    )

    # Deltas vs previous window ------------------------------------------------------------
    if not prev_df.empty:
        prev_sessions = safe_sum(prev_df.get("sessions", pd.Series(dtype=float)))
        prev_conversions = safe_sum(prev_df.get("conversions", pd.Series(dtype=float)))
        prev_ads_clicks = safe_sum(prev_df.get("clicks_ads", pd.Series(dtype=float)))
        prev_sc_clicks = safe_sum(prev_df.get("clicks_sc", pd.Series(dtype=float)))

        metrics.update(
            {
                "sessions_delta": tot_sessions - prev_sessions,
                "conversions_delta": tot_conv - prev_conversions,
                "ads_clicks_delta": tot_ads_clicks - prev_ads_clicks,
                "search_clicks_delta": tot_sc_clicks - prev_sc_clicks,
            }
        )
    else:
        metrics.update(
            {
                "sessions_delta": 0,
                "conversions_delta": 0,
                "ads_clicks_delta": 0,
                "search_clicks_delta": 0,
            }
        )

    # Correlations -------------------------------------------------------------------------
    metrics["correlations"] = compute_correlations(cur_df)

    # ------------------------------------------------------------
    # Pull L5-session metrics from Google Ads JSON summary
    # ------------------------------------------------------------
    try:
        suffix = f"{window_days}d"
        ads_json_path = os.path.join(SUMMARY_DIR, ADS_JSON_PATTERN.format(suffix=suffix))
        if os.path.exists(ads_json_path):
            import json as _json
            with open(ads_json_path) as _f:
                ads_js = _json.load(_f)
            l5_val = ads_js.get("l5_sessions")
            cost_l5 = ads_js.get("cost_per_l5")
            metrics["ads_l5_sessions"] = l5_val
            metrics["ads_cost_per_l5"] = cost_l5
    except Exception:
        metrics["ads_l5_sessions"] = None
        metrics["ads_cost_per_l5"] = None

    return metrics


# ---------------------------------------------------------------------------
# Writer helpers
# ---------------------------------------------------------------------------

def write_json_summary(metrics: dict[str, Any], out_path: str):
    import pathlib
    pathlib.Path(out_path).write_text(json.dumps(metrics, default=str, indent=2))


def write_markdown_summary(metrics: dict[str, Any], out_path: str, narrative: str | None = None):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    with open(out_path, "w") as f:
        f.write("---\n")
        f.write("source: combined\n")
        f.write(f"date_range: {metrics['date_range']}\n")
        f.write(f"generated_at: {now}\n")
        f.write(f"window_days: {metrics['window_days']}\n")
        f.write("---\n\n")
        f.write("# Combined Marketing & Site Funnel Summary\n\n")

        # High-level KPIs
        f.write("## Totals\n")
        f.write(f"- **Sessions:** {metrics['sessions']:,}\n")
        f.write(f"- **Conversions:** {metrics['conversions']:,}\n")
        f.write(f"- **Conversion Rate:** {metrics['conversion_rate_pct']:.2f}%\n")
        f.write("\n## Google Ads\n")
        f.write(f"- **Impressions:** {metrics['ads_impressions']:,}\n")
        f.write(f"- **Clicks:** {metrics['ads_clicks']:,}\n")
        f.write(f"- **CTR:** {metrics['ads_ctr']:.2f}%\n")
        f.write(f"- **Cost:** £{metrics['ads_cost']:.2f}\n")
        if metrics.get('ads_cpc') is not None:
            f.write(f"- **CPC:** £{metrics['ads_cpc']:.2f}\n")
        if metrics.get('ads_cpa') is not None:
            f.write(f"- **CPA:** £{metrics['ads_cpa']:.2f}\n")
        if metrics.get('cost_per_session') is not None:
            f.write(f"- **Cost per Session:** £{metrics['cost_per_session']:.2f}\n")

        f.write("\n## Search Console\n")
        f.write(f"- **Impressions:** {metrics['search_impressions']:,}\n")
        f.write(f"- **Clicks:** {metrics['search_clicks']:,}\n")
        f.write(f"- **CTR:** {metrics['search_ctr']:.2f}%\n")

        # Correlations
        if metrics.get("correlations"):
            f.write("\n## Funnel Correlations (Pearson r)\n")
            for name, val in metrics["correlations"].items():
                if val is not None:
                    f.write(f"- **{name.replace('_', ' ').title()}:** {val:+.2f}\n")

        if narrative:
            f.write("\n---\n\n## Narrative Summary\n\n")
            f.write(narrative.strip() + "\n")


# ---------------------------------------------------------------------------
# Narrative (LLM) helper
# ---------------------------------------------------------------------------

def generate_narrative(metrics: dict[str, Any]) -> str | None:
    if not openai.api_key:
        logger.warning("OPENAI_API_KEY not set – skipping narrative generation.")
        return None
    prompt = (
        "You are a marketing analytics assistant. Craft a brief executive summary (≈3 sentences) covering: "
        "overall performance changes, noteworthy channel interactions (e.g. ads clicks ⇨ sessions), and spend efficiency.\n"
        f"Metrics JSON: {json.dumps(metrics, default=str)}"
    )
    try:
        resp = openai.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=120,
        )
        return resp.choices[0].message.content
    except Exception as e:
        logger.error(f"Narrative generation failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------

def generate_summaries():
    for days in [30, 90, 365]:
        metrics = build_metrics_from_json(days)
        narrative = generate_narrative(metrics)
        suffix = f"{days}d"
        out_md = Path(SUMMARY_DIR) / f"combined_summary_{suffix}.md"
        write_markdown_summary(metrics, str(out_md), narrative=narrative)
        out_json = Path(SUMMARY_DIR) / f"combined_summary_{suffix}.json"
        write_json_summary(metrics, str(out_json))
        print(f"Combined summary written to {out_md}")


def main():
    os.makedirs(SUMMARY_DIR, exist_ok=True)
    generate_summaries()


if __name__ == "__main__":
    main() 