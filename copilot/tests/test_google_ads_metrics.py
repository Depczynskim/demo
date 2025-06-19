import os, sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summarization')))
from google_ads_summary import compute_metrics, find_latest_parquet, DATA_DIR


def test_google_ads_metrics_real_data():
    latest_file = find_latest_parquet(DATA_DIR)
    assert latest_file is not None, "No Google Ads parquet file found."
    df = pd.read_parquet(latest_file)
    metrics = compute_metrics(df, date_window=7)
    assert metrics['total_impressions'] >= 0
    assert metrics['window_days'] == 7
    assert 'ctr' in metrics 