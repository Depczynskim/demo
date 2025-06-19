import os
import json
import glob
from datetime import datetime
from typing import Dict, Any, List

SUMMARY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summaries'))
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'context'))
WINDOWS: List[int] = [30, 90, 365]
SOURCES = {
    'ga4': 'ga4_summary_{suffix}.json',
    'google_ads': 'google_ads_summary_{suffix}.json',
    'search_console': 'search_console_summary_{suffix}.json',
    'combined': 'combined_summary_{suffix}.json',
}

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r') as f:
        return json.load(f)


def build_context_for_window(days: int) -> Dict[str, Any]:
    suffix = f"{days}d"
    ctx: Dict[str, Any] = {
        'window_days': days,
        'generated_at': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
    }
    missing: List[str] = []
    for source, tmpl in SOURCES.items():
        expected_path = os.path.join(SUMMARY_DIR, tmpl.format(suffix=suffix))
        if not os.path.isfile(expected_path):
            missing.append(expected_path)
            continue
        ctx[source] = load_json(expected_path)
    if missing:
        ctx['missing_sources'] = missing  # include list for debugging, but still return context
    return ctx


def main():
    for days in WINDOWS:
        context = build_context_for_window(days)
        out_path = os.path.join(OUTPUT_DIR, f'context_{days}d.json')
        with open(out_path, 'w') as f:
            json.dump(context, f, indent=2, default=str)
        print(f'Context package written to {out_path}')


if __name__ == '__main__':
    main() 