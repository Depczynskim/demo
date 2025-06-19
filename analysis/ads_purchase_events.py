import glob, pandas as pd
paths = glob.glob('data_repo/ga4/analytics_events_final/report_month=*/data_*.parquet')
unique=set()
for p in paths:
    df=pd.read_parquet(p, columns=['event_name'])
    unique.update(df['event_name'].unique())
for name in sorted(unique):
    if 'conversion' in name.lower() or 'purchase' in name.lower():
        print(name) 