import os
import glob
from datetime import datetime
import json
from typing import Any

import pandas as pd
import openai
from dotenv import load_dotenv

# Utils
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from logger import get_logger

# Load env and logger
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Session-level helper – engagement levels & country splits
# ---------------------------------------------------------------------------


def build_session_df(events: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with one row per GA-4 session including engagement level.

    Columns returned: ga_session_id, date (YYYY-MM-DD), geo_country, traffic_medium,
    traffic_source, device_category, engagement_level (1-6).
    """

    if events.empty:
        return pd.DataFrame()

    # helper to extract ga_session_id from event_params_json if not present as col
    if "ga_session_id" not in events.columns:
        def _extract_sid(val):
            if pd.isna(val):
                return None
            try:
                params = json.loads(val)
                for p in params:
                    if p.get("key") == "ga_session_id":
                        v = p.get("value", {})
                        return (
                            v.get("int_value")
                            or v.get("string_value")
                            or v.get("float_value")
                            or v.get("double_value")
                        )
            except Exception:
                return None
            return None

        events = events.copy()
        events["ga_session_id"] = events["event_params_json"].apply(_extract_sid)

    events = events[events["ga_session_id"].notna()].copy()

    # Engagement-level classifier --------------------------------------------------
    def _level(sess: pd.Series) -> int:
        evts = set(sess)
        # Level 6 – purchase
        if "purchase" in evts:
            return 6
        # Level 5 – add-to-cart, form, checkout
        if evts & {"add_to_cart", "view_cart", "form_start", "begin_checkout"}:
            return 5
        # Level 4 – onsite search or ≥3 products viewed (approx: view_item)
        if "search" in evts or (sess == "view_item").sum() >= 3:
            return 4
        # Level 3 – faq, gallery, scroll depth proxy
        if evts & {"faq_interaction", "photo_gallery_click", "scroll"}:
            return 3
        # Level 2 – >1 page or >60s engaged – lacking duration per session here; use page_view count
        if (sess == "page_view").sum() > 1:
            return 2
        return 1

    grouped = events.groupby("ga_session_id")
    sess_df = grouped["event_name"].apply(_level).to_frame("engagement_level")

    # first/major attributes
    attrs = grouped.agg(
        date=("event_date", "first"),
        geo_country=("geo_country", "first"),
        traffic_medium=("traffic_medium", "first"),
        traffic_source=("traffic_source", "first"),
        device_category=("device_category", "first"),
    )

    sess_df = sess_df.join(attrs)
    # Convert date to YYYY-MM-DD string
    sess_df["date"] = pd.to_datetime(sess_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return sess_df.reset_index()

# Path to persist the latest session-level parquet (overwritten each run)
SESSIONS_PARQUET = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data_repo", "ga4", "sessions_latest.parquet"))

# Output directory: copilot/summaries (sibling folder to this module's parent)
SUMMARY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summaries'))
DATA_DIR = "data_repo/ga4/analytics_events_final"


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


def compute_metrics(df, date_window: int | None = None):
    """Compute key GA4 product analytics metrics."""
    metrics = {}
    metrics['total_events'] = len(df)

    # Basic entity counts -------------------------------------------------------------------
    metrics['unique_users'] = (
        df['user_pseudo_id'].nunique() if 'user_pseudo_id' in df.columns else None
    )

    # Sessions: GA4 exports typically contain one `session_start` event per session.
    if 'event_name' in df.columns:
        metrics['sessions'] = int((df['event_name'] == 'session_start').sum())
        metrics['page_views'] = int((df['event_name'] == 'page_view').sum())
    else:
        metrics['sessions'] = metrics['page_views'] = None

    # Avg engagement time (sec) --------------------------------------------------------------
    metrics['avg_engagement_time_sec'] = None
    if 'engagement_time_msec' in df.columns:
        metrics['avg_engagement_time_sec'] = float(df['engagement_time_msec'].dropna().mean() / 1000)
    elif 'event_params_json' in df.columns:
        # Parse engagement_time_msec from the param JSON only for a sample to avoid heavy parsing
        def _extract_time(val):
            try:
                params = json.loads(val)
            except Exception:
                return None
            for p in params:
                if p.get('key') == 'engagement_time_msec':
                    v = p.get('value', {})
                    return (
                        v.get('int_value') or v.get('float_value') or v.get('double_value') or v.get('string_value')
                    )
            return None

        times = df['event_params_json'].dropna().head(10000).map(_extract_time).dropna().astype(float)  # sample up to 10k rows
        if not times.empty:
            metrics['avg_engagement_time_sec'] = float(times.mean() / 1000)

    # ------------------------------------------------------------------
    # Timing patterns – top days of week & hours (UTC for simplicity)
    # ------------------------------------------------------------------
    if 'event_timestamp' in df.columns:
        # Convert microsecond timestamp -> UTC -> Europe/London (handles DST automatically)
        ts_utc = pd.to_datetime(df['event_timestamp'] // 1_000_000, unit='s', utc=True)
        ts = ts_utc.dt.tz_convert('Europe/London')

        day_counts = ts.dt.day_name().value_counts().head(3)
        metrics['top_days'] = day_counts.to_dict()

        hour_counts = ts.dt.hour.value_counts().head(5)
        metrics['top_hours'] = hour_counts.to_dict()

        # Hour breakdown for the single top day
        if not day_counts.empty:
            top_day_name = day_counts.index[0]
            mask = ts.dt.day_name() == top_day_name
            top_day_hours = ts[mask].dt.hour.value_counts().head(5)
            metrics['top_hours_for_top_day'] = {
                'day': top_day_name,
                'hours': top_day_hours.to_dict()
            }
    else:
        metrics['top_days'] = metrics['top_hours'] = metrics['top_hours_for_top_day'] = None

    # ------------------------------------------------------------------
    # Search terms & FAQ interactions
    # ------------------------------------------------------------------
    if 'event_params_json' in df.columns:
        import json as _json

        def _extract_param(json_str, key):
            try:
                if pd.isna(json_str):
                    return None
                params = _json.loads(json_str)
                for p in params:
                    if p.get('key') == key:
                        v = p.get('value', {})
                        return (
                            v.get('string_value') or v.get('int_value') or v.get('float_value') or v.get('double_value')
                        )
                return None
            except Exception:
                return None

        # Search terms ----------------------------------------------
        searches = df[df['event_name'] == 'search'].copy()
        if not searches.empty:
            searches['search_term'] = searches['event_params_json'].apply(lambda x: _extract_param(x, 'search_term'))
            top_terms = searches['search_term'].dropna().value_counts().head(5)
            metrics['top_search_terms'] = top_terms.to_dict()
        else:
            metrics['top_search_terms'] = None

        # FAQ interactions -------------------------------------------
        faq_df = df[df['event_name'] == 'faq_interaction'].copy()
        if not faq_df.empty:
            faq_df['faq_question'] = faq_df['event_params_json'].apply(lambda x: _extract_param(x, 'faq_question'))
            top_questions = faq_df['faq_question'].dropna().value_counts().head(5)
            metrics['top_faq_questions'] = top_questions.to_dict()
        else:
            metrics['top_faq_questions'] = None
    else:
        metrics['top_search_terms'] = metrics['top_faq_questions'] = None

    # Top products by views ------------------------------------------------------------------
    if 'page_title' in df.columns:
        top_products = df[df['page_type'] == 'Product Pages']['page_title'].value_counts().head(5)
        metrics['top_products'] = top_products
    else:
        metrics['top_products'] = None

    # Top countries + initialise breakdowns dict ---------------------------------
    if 'geo_country' in df.columns:
        top_countries = df['geo_country'].value_counts().head(5)
        metrics['top_countries'] = top_countries
    else:
        top_countries = pd.Series(dtype=int)
        metrics['top_countries'] = None

    # ------------------------------------------------------------
    # Per-country breakdown helper
    # ------------------------------------------------------------

    def _country_metrics(c_df: pd.DataFrame):
        c_d: dict[str, Any] = {}

        # Channels ----------------------------------------------
        if 'traffic_source' in c_df.columns:
            c_d['top_channels'] = c_df['traffic_source'].value_counts().head(5).to_dict()

        # Traffic source / medium pairs -------------------------
        if {'traffic_source', 'traffic_medium'}.issubset(c_df.columns):
            pairs = (
                c_df[['traffic_source', 'traffic_medium']]
                .dropna()
                .value_counts()
                .head(5)
                .reset_index()
                .rename(columns={0: 'count'})
            )
            c_d['top_traffic_sources'] = pairs.to_dict(orient='records')

        # Timing patterns (top days & hours) --------------------
        if 'event_timestamp' in c_df.columns:
            ts_utc = pd.to_datetime(c_df['event_timestamp'] // 1_000_000, unit='s', utc=True)
            ts_loc = ts_utc.dt.tz_convert('Europe/London')  # use London as baseline
            c_d['top_days'] = ts_loc.dt.day_name().value_counts().head(3).to_dict()

            if not ts_loc.empty:
                best_day = ts_loc.dt.day_name().value_counts().idxmax()
                best_mask = ts_loc.dt.day_name() == best_day
                c_d['peak_hours_best_day'] = {
                    'day': best_day,
                    'hours': ts_loc[best_mask].dt.hour.value_counts().head(5).to_dict(),
                }

        # Search terms & FAQ questions --------------------------
        if not c_df.empty and 'event_params_json' in c_df.columns:
            def _extract_param(val, key):
                try:
                    if pd.isna(val):
                        return None
                    _p = _json.loads(val)
                    for prm in _p:
                        if prm.get('key') == key:
                            v = prm.get('value', {})
                            return (
                                v.get('string_value')
                                or v.get('int_value')
                                or v.get('float_value')
                                or v.get('double_value')
                            )
                    return None
                except Exception:
                    return None

            searches = c_df[c_df['event_name'] == 'search'].copy()
            if not searches.empty:
                searches['search_term'] = searches['event_params_json'].apply(lambda x: _extract_param(x, 'search_term'))
                c_d['top_search_terms'] = searches['search_term'].dropna().value_counts().head(5).to_dict()

            faq = c_df[c_df['event_name'] == 'faq_interaction'].copy()
            if not faq.empty:
                faq['faq_question'] = faq['event_params_json'].apply(lambda x: _extract_param(x, 'faq_question'))
                faq['faq_product'] = faq['event_params_json'].apply(lambda x: _extract_param(x, 'ecomm_prodid'))

                # Build map product id -> title
                id_to_title: dict[str, str] = {}
                if {'ecomm_prodid', 'page_title'}.issubset(c_df.columns):
                    _map_df = c_df[['ecomm_prodid', 'page_title']].dropna().drop_duplicates()
                    _map_df['clean_title'] = _map_df['page_title'].str.replace(r'^POPS: Buy From Makers - ', '', regex=True).str.strip()
                    id_to_title = pd.Series(_map_df.clean_title.values, index=_map_df.ecomm_prodid.astype(str)).to_dict()

                topq = (
                    faq.groupby(['faq_question', 'faq_product']).size().reset_index(name='clicks')
                    .sort_values('clicks', ascending=False).head(5)
                )
                c_d['top_faq_questions'] = [
                    {
                        'question': row['faq_question'],
                        'product_id': row['faq_product'],
                        'product_name': id_to_title.get(str(row['faq_product']), str(row['faq_product'])),
                        'clicks': int(row['clicks']),
                    }
                    for _, row in topq.iterrows()
                ]

        return c_d

    # ------------------------------------------------------------
    # Build per-country breakdowns
    # ------------------------------------------------------------

    country_breakdowns: dict[str, Any] = {}
    for country in top_countries.index:
        c_df = df[df['geo_country'] == country]
        cb = _country_metrics(c_df)
        # Engagement level counts within window (requires engagement_level col)
        if 'engagement_level' in c_df.columns:
            cb['level_counts'] = c_df['engagement_level'].value_counts().to_dict()
        country_breakdowns[country] = cb

    metrics['country_breakdowns'] = country_breakdowns

    # Top channels & src/medium globally (entire df)
    if 'traffic_source' in df.columns:
        metrics['top_channels'] = df['traffic_source'].value_counts().head(5).to_dict()

    if {'traffic_source', 'traffic_medium'}.issubset(df.columns):
        pairs_glob = (
            df[['traffic_source', 'traffic_medium']]
            .dropna()
            .value_counts()
            .head(5)
            .reset_index()
            .rename(columns={0: 'count'})
        )
        metrics['top_traffic_sources'] = pairs_glob.to_dict(orient='records')
    else:
        metrics['top_traffic_sources'] = None

    # Top products by conversions (if available)
    if 'event_name' in df.columns and 'page_title' in df.columns:
        conversions = df[df['event_name'].str.contains('purchase|conversion', case=False, na=False)]
        top_products_conv = conversions['page_title'].value_counts().head(5)
        metrics['top_products_conversions'] = top_products_conv
    else:
        metrics['top_products_conversions'] = None

    # Date range
    if 'event_date' in df.columns:
        min_date_dt = pd.to_datetime(df['event_date']).min()
        max_date_dt = pd.to_datetime(df['event_date']).max()
    elif 'date' in df.columns:
        min_date_dt = pd.to_datetime(df['date']).min()
        max_date_dt = pd.to_datetime(df['date']).max()
    else:
        min_date_dt = max_date_dt = None

    import pandas as _pd
    if (min_date_dt is not None and max_date_dt is not None and not _pd.isna(min_date_dt) and not _pd.isna(max_date_dt)):
        metrics['date_range'] = f"{min_date_dt.strftime('%Y-%m-%d')} to {max_date_dt.strftime('%Y-%m-%d')}"
    else:
        metrics['date_range'] = None

    # Add metadata about the window
    metrics['window_days'] = date_window

    # Nullify additional behaviour metrics globally
    metrics['top_days'] = None
    metrics['top_hours_for_top_day'] = None
    metrics['top_search_terms'] = None
    metrics['top_faq_questions'] = None

    return metrics


def write_markdown_summary(metrics: dict[str, Any], output_path: str, narrative: str | None = None):
    """Write a Markdown summary file with front-matter metadata and metrics."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    with open(output_path, 'w') as f:
        f.write(f"---\n")
        f.write(f"source: ga4\n")
        if metrics['date_range']:
            f.write(f"date_range: {metrics['date_range']}\n")
        f.write(f"generated_at: {now}\n")
        if metrics.get('window_days'):
            f.write(f"window_days: {metrics['window_days']}\n")
        f.write(f"---\n\n")
        f.write(f"# GA4 Product Analytics Summary\n\n")
        f.write(f"- **Total Events:** {metrics['total_events']:,}\n")
        if metrics['unique_users'] is not None:
            f.write(f"- **Unique Users:** {metrics['unique_users']:,}\n")
        if metrics.get('sessions') is not None:
            f.write(f"- **Sessions:** {metrics['sessions']:,}\n")
        if metrics.get('page_views') is not None:
            f.write(f"- **Page Views:** {metrics['page_views']:,}\n")
        if metrics.get('avg_engagement_time_sec') is not None:
            f.write(f"- **Avg Engagement Time:** {metrics['avg_engagement_time_sec']:.1f} sec\n")
        if metrics['top_products'] is not None:
            f.write(f"\n## Top 5 Products by Views\n")
            for prod, count in metrics['top_products'].items():
                f.write(f"- {prod}: {count:,} views\n")
        if metrics['top_products_conversions'] is not None and not metrics['top_products_conversions'].empty:
            f.write(f"\n## Top 5 Products by Conversions\n")
            for prod, count in metrics['top_products_conversions'].items():
                f.write(f"- {prod}: {count:,} conversions\n")
        if metrics['top_countries'] is not None:
            f.write(f"\n## Top 5 Countries\n")
            for country, count in metrics['top_countries'].items():
                f.write(f"- {country}: {count:,} events\n")

        # --------------------------------------------------------
        # Per-country deep dive sections
        # --------------------------------------------------------
        if metrics.get('country_breakdowns'):
            for ctry, cdata in metrics['country_breakdowns'].items():
                f.write(f"\n---\n\n## Country Spotlight: {ctry}\n")
                if cdata.get('top_channels'):
                    f.write("\n### Top Channels\n")
                    for ch, ct in cdata['top_channels'].items():
                        f.write(f"- {ch}: {ct:,} events\n")
                if cdata.get('top_traffic_sources'):
                    f.write("\n### Top Source / Medium Pairs\n")
                    for row in cdata['top_traffic_sources']:
                        f.write(f"- {row['traffic_source']} / {row['traffic_medium']}: {row['count']:,}\n")
                if cdata.get('top_days'):
                    f.write("\n### Top Days\n")
                    for d,ct in cdata['top_days'].items():
                        f.write(f"- {d}: {ct:,} events\n")
                if cdata.get('peak_hours_best_day') and isinstance(cdata['peak_hours_best_day'], dict):
                    ph = cdata['peak_hours_best_day']
                    if 'day' in ph and isinstance(ph['day'], str):
                        f.write(f"\n### Peak Hours on {ph['day']}\n")
                        hours_map = ph.get('hours', {}) if isinstance(ph.get('hours'), dict) else {}
                    else:
                        f.write("\n### Peak Hours on Best Day\n")
                        hours_map = ph
                    for hr, ct in hours_map.items():
                        try:
                            hr_int = int(hr)
                            f.write(f"- {hr_int:02d}:00 – {ct:,} events\n")
                        except Exception:
                            continue
                if cdata.get('top_search_terms'):
                    f.write("\n### Top Search Terms\n")
                    for term, ct in cdata['top_search_terms'].items():
                        f.write(f"- {term}: {ct:,} searches\n")
                if cdata.get('top_faq_questions'):
                    f.write("\n### FAQ Clicks\n")
                    for row in cdata['top_faq_questions']:
                        prod_display = row.get('product_name') or row.get('product_id')
                        f.write(f"- {row['question']} ({prod_display}): {row['clicks']:,} clicks\n")

        # --------------------------
        # Narrative summary AT THE END
        # --------------------------
        if narrative:
            f.write("\n---\n\n")
            f.write("## Narrative Summary\n\n")
            f.write(narrative.strip() + "\n")


def generate_summaries(df):
    """Generate summaries for multiple rolling windows."""
    # Use 30-day short-term window instead of 7 days so that all Copilot
    # components align on the same reporting cadence (30/90/365-day).
    windows = [30, 90, 365]
    for days in windows:
        # Filter dataframe to last <days> days if date column available
        date_col = None
        for possible in ['event_date', 'date']:
            if possible in df.columns:
                date_col = possible
                break
        if date_col:
            # Ensure both series and cutoff are timezone-aware (UTC) to avoid invalid comparisons.
            dt_series = pd.to_datetime(df[date_col], errors='coerce')
            dt_ns = dt_series.view('int64')  # nanoseconds since epoch; NaT -> -9223372036854775808
            # First pass: relative to today
            cutoff_ns_today = (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).value
            df_filtered = df[dt_ns >= cutoff_ns_today]

            # Fallback: take the last <days> relative to the most recent date in dataset
            if df_filtered.empty and not dt_series.dropna().empty:
                max_ts = dt_series.max()
                cutoff_ns_recent = (max_ts - pd.Timedelta(days=days)).value
                df_filtered = df[dt_ns >= cutoff_ns_recent]
        else:
            df_filtered = df.copy()

        # ------------------------------------------------------------------
        # Build *previous* window dataframe (days → 2×days range) to calculate
        # deltas so the Copilot can contextualise KPIs.
        # ------------------------------------------------------------------
        prev_df = pd.DataFrame()
        if date_col:
            # Determine sliding window relative to the most-recent timestamp
            max_ts = pd.to_datetime(df[date_col], errors='coerce').max()
            if pd.notna(max_ts):
                prev_upper_ns = (max_ts - pd.Timedelta(days=days)).value
                prev_lower_ns = (max_ts - pd.Timedelta(days=2 * days)).value
                date_ns_all = pd.to_datetime(df[date_col], errors='coerce').view('int64')
                prev_mask = (date_ns_all >= prev_lower_ns) & (date_ns_all < prev_upper_ns)
                prev_df = df[prev_mask]

        metrics = compute_metrics(df_filtered, date_window=days)

        # ------------------------------------------------------------------
        # Behavioural signal lifts – compare prevalence in Level-5 sessions
        # vs. all other sessions.
        # ------------------------------------------------------------------
        try:
            sess_df_full = build_session_df(df_filtered)

            # ------------------------------------------------------------
            # GLOBAL ENGAGEMENT-LEVEL COUNTS
            # ------------------------------------------------------------
            if not sess_df_full.empty and 'engagement_level' in sess_df_full.columns:
                # Store as simple "level -> count" dict, e.g. {3: 42, 4: 17, 5: 6}
                metrics['level_counts'] = (
                    sess_df_full['engagement_level']
                    .value_counts()
                    .sort_index()  # deterministic order – 1..6
                    .to_dict()
                )

            if not sess_df_full.empty and 'ga_session_id' in sess_df_full.columns:
                # Signals → set(session_ids)
                sig_sets: dict[str, set] = {}

                # Extract ga_session_id column into events df for join; compute if missing
                if 'ga_session_id' not in df_filtered.columns:
                    # Re-use helper inside build_session_df to extract; already added consequently above.
                    def _extract_sid2(val):
                        if pd.isna(val):
                            return None
                        try:
                            params = json.loads(val)
                            for p in params:
                                if p.get('key') == 'ga_session_id':
                                    v = p.get('value', {})
                                    return (
                                        v.get('int_value')
                                        or v.get('string_value')
                                        or v.get('float_value')
                                        or v.get('double_value')
                                    )
                        except Exception:
                            return None
                        return None

                    df_filtered = df_filtered.copy()
                    df_filtered['ga_session_id'] = df_filtered['event_params_json'].apply(_extract_sid2)

                # FAQ clicks -------------------------------------------------
                faq_sessions = set(df_filtered[df_filtered['event_name'] == 'faq_interaction']['ga_session_id'].dropna().unique())
                sig_sets['faq_click'] = faq_sessions

                # Photo-gallery clicks --------------------------------------
                gallery_sessions = set(df_filtered[df_filtered['event_name'] == 'photo_gallery_click']['ga_session_id'].dropna().unique())
                sig_sets['gallery_click'] = gallery_sessions

                # ≥3 page-views ---------------------------------------------
                pv_counts = (
                    df_filtered[df_filtered['event_name'] == 'page_view']
                    .groupby('ga_session_id')
                    .size()
                )
                three_pv_sessions = set(pv_counts[pv_counts >= 3].index)
                sig_sets['three_pageviews'] = three_pv_sessions

                # On-site search -------------------------------------------
                search_sessions = set(df_filtered[df_filtered['event_name'] == 'search']['ga_session_id'].dropna().unique())
                sig_sets['onsite_search'] = search_sessions

                total_sessions_set = set(sess_df_full['ga_session_id'])
                l5_sessions_set = set(sess_df_full[sess_df_full['engagement_level'] == 5]['ga_session_id'])

                lifts: dict[str, float | None] = {}
                for name, ss in sig_sets.items():
                    if not ss:
                        lifts[name] = None
                        continue
                    # Probabilities
                    p_l5 = len(l5_sessions_set & ss) / len(l5_sessions_set) if l5_sessions_set else 0
                    non_l5_set = total_sessions_set - l5_sessions_set
                    p_non = len(non_l5_set & ss) / len(non_l5_set) if non_l5_set else 0
                    lifts[name] = round(p_l5 / p_non, 2) if p_non else None

                metrics['signal_lifts'] = lifts
        except Exception as _exc:
            logger.warning(f"Lift calculation failed: {_exc}")

        # ---------------------------------------------
        # Compute deltas vs previous window
        # ---------------------------------------------
        if not prev_df.empty:
            prev_metrics = compute_metrics(prev_df, date_window=days)

            def _delta(cur, prev):
                if cur is None or prev is None:
                    return None
                try:
                    return cur - prev
                except Exception:
                    return None

            delta_map = {
                'total_events': 'total_events_delta',
                'unique_users': 'unique_users_delta',
                'sessions': 'sessions_delta',
                'page_views': 'page_views_delta',
                'avg_engagement_time_sec': 'avg_engagement_time_sec_delta',
            }

            for k_cur, k_delta in delta_map.items():
                metrics[k_delta] = _delta(metrics.get(k_cur), prev_metrics.get(k_cur))
        else:
            # Set to None when we don't have previous data
            for suffix in ['total_events', 'unique_users', 'sessions', 'page_views', 'avg_engagement_time_sec']:
                metrics[f"{suffix}_delta"] = None

        suffix = f"{days}d"
        output_path = os.path.join(SUMMARY_DIR, f"ga4_summary_{suffix}.md")
        narrative = generate_narrative(metrics)
        write_markdown_summary(metrics, output_path, narrative=narrative)

        # Also write JSON alongside
        json_path = os.path.join(SUMMARY_DIR, f"ga4_summary_{suffix}.json")
        write_json_summary(metrics, json_path)

        print(f"Summary written to {output_path}")


def generate_narrative(metrics: dict[str, Any]) -> str | None:
    """Generate a concise natural-language narrative using OpenAI. Returns None on failure."""
    if not openai.api_key:
        logger.warning("OPENAI_API_KEY not set – skipping narrative generation.")
        return None
    prompt = (
        "You are an e-commerce analytics assistant. Craft a concise, plain-English executive summary (≈3 sentences) "
        "highlighting: 1) the biggest shifts in user behaviour, 2) the most important country-level differences "
        "(traffic sources, timing, search), and 3) any standout products or FAQs.\n"
        "Start with the overall insight, then call out one country example.\n"
        f"Metrics JSON: {json.dumps(metrics, default=str)}"
    )
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=120,
        )
        narrative = response.choices[0].message.content
        return narrative
    except Exception as e:
        logger.error(f"Narrative generation failed: {e}")
        return None


def write_json_summary(metrics: dict[str, Any], output_path: str):
    """Write metrics dict as prettified JSON."""
    import json, pathlib
    pathlib.Path(output_path).write_text(json.dumps(metrics, default=str, indent=2))


def main():
    os.makedirs(SUMMARY_DIR, exist_ok=True)
    latest_file = find_latest_parquet(DATA_DIR)
    if not latest_file:
        print("No GA4 parquet file found.")
        return
    df = pd.read_parquet(latest_file)
    generate_summaries(df)


if __name__ == "__main__":
    main() 