#!/usr/bin/env python3
"""
Preprocess reviews.csv rows into a compact TSV:

Input (CSV): listing_id, id (review_id), date, ...
Output (TSV): listing_id\treview_id\tdate\tyyyy-mm

Notes:
- Robust to missing columns; skips rows without listing_id.
"""
import sys
import csv

reader = csv.DictReader(sys.stdin)
for row in reader:
    try:
        lid = (row.get("listing_id") or "").strip()
        if not lid:
            continue
        rid = (row.get("id") or row.get("review_id") or "").strip()
        date = (row.get("date") or "").strip()
        ym = date[:7] if len(date) >= 7 else ""
        print(f"{lid}\t{rid}\t{date}\t{ym}")
    except Exception:
        continue
