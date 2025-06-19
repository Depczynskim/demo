import glob, pandas as pd

paths = glob.glob('data_repo/ga4/analytics_events_final/report_month=*/data_*.parquet')
total = {}
for p in paths:
    df = pd.read_parquet(p, columns=['event_name'])
    vc = df['event_name'].value_counts()
    for name, cnt in vc.items():
        if 'purchase' in name.lower():
            total[name] = total.get(name, 0) + cnt

print('\nPurchase-related events across all months:')
for name, cnt in sorted(total.items(), key=lambda x: -x[1]):
    print(f'{name:<40s} {cnt}') 