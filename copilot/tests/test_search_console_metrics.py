import os, sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summarization')))
from search_console_summary import compute_metrics, find_latest_parquet, DATA_DIR

def test_search_console_metrics_real_data():
    latest_file = find_latest_parquet(DATA_DIR)
    assert latest_file is not None, "No Search Console parquet file found."
    df = pd.read_parquet(latest_file)
    # Provide an empty previous_df for simple sanity check
    prev_df = pd.DataFrame(columns=df.columns)
    metrics = compute_metrics(df, prev_df, date_window=7)
    assert metrics['clicks'] >= 0
    assert metrics['window_days'] == 7
    assert 'ctr' in metrics 