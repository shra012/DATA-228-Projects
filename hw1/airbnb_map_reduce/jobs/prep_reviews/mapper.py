#!/usr/bin/env python3
"""
Preprocess reviews.csv rows into a compact TSV:

Input (CSV): listing_id, date, ...
Output (TSV): listing_id\tdate\tyyyy-mm

Notes:
- Robust to missing columns; skips rows without listing_id.
"""
import sys, csv

reader = csv.DictReader(sys.stdin)
for row in reader:
    try:
        lid = (row.get("listing_id") or "").strip()
        if not lid:
            continue
        date = (row.get("date") or "").strip()
        ym = date[:7] if len(date) >= 7 else ""
        print(f"{lid}\t{date}\t{ym}")
    except Exception:
        continue

