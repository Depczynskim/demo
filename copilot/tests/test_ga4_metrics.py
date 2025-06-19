import os, sys
import pandas as pd

# Ensure summarization modules are importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summarization')))

from ga4_summary import compute_metrics, find_latest_parquet, DATA_DIR

def test_compute_metrics_real_data():
    """Compute metrics on the latest real GA4 parquet file and check expected keys exist."""
    latest_file = find_latest_parquet(DATA_DIR)
    assert latest_file is not None, "No GA4 parquet file found in data_repo."
    df = pd.read_parquet(latest_file)
    metrics = compute_metrics(df, date_window=7)
    # Basic sanity checks
    assert metrics['total_events'] > 0, "Total events should be > 0 on real data."
    assert metrics['unique_users'] is None or metrics['unique_users'] > 0
    assert metrics['window_days'] == 7
    assert 'top_products' in metrics 