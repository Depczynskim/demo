import os, glob, pandas as pd
from pathlib import Path

# Re-use build_session_df from ga4_summary
import sys
from importlib import import_module

# Ensure repository root is on PYTHONPATH
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SUMMARY_MOD = import_module('copilot.summarization.ga4_summary')
build_session_df = getattr(SUMMARY_MOD, 'build_session_df')

DATA_DIR = Path('data_repo/ga4/analytics_events_final')
OUTPUT_PATH = Path('data_repo/ga4/sessions_latest.parquet')


def find_latest_files(base: Path):
    return glob.glob(str(base / 'report_month=*' / 'data_*.parquet'))


def load_events():
    files = find_latest_files(DATA_DIR)
    if not files:
        raise FileNotFoundError('No GA4 parquet files found.')
    cols = [
        'event_name', 'event_params_json', 'event_date', 'event_timestamp',
        'user_pseudo_id', 'geo_country', 'traffic_source', 'traffic_medium', 'device_category'
    ]
    parts = [pd.read_parquet(f, columns=cols) for f in files]
    return pd.concat(parts, ignore_index=True)


def main():
    ds = load_events()
    sess = build_session_df(ds)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    sess.to_parquet(OUTPUT_PATH, index=False)
    print(f'Wrote {len(sess):,} sessions to {OUTPUT_PATH}')


if __name__ == '__main__':
    main() 