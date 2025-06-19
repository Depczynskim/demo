import os, glob, pandas as pd

BASE_PATH = 'data_repo/ga4/analytics_events_final'
paths = glob.glob(f"{BASE_PATH}/report_month=*/data_*.parquet")
if not paths:
    print('No GA4 parquet found')
    raise SystemExit

counts = {}
for p in paths:
    month = os.path.basename(os.path.dirname(p))
    df = pd.read_parquet(p, columns=['event_name'])
    counts[month] = df['event_name'].value_counts()

all_counts = pd.concat(counts.values()).groupby(level=0).sum()
print('Top event_name counts:')
print(all_counts.sort_values(ascending=False).head(20))
print('\nPurchase events across months:')
print({m: counts[m].get('purchase', 0) for m in counts}) 