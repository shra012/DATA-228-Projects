#!/usr/bin/env python3
"""
Mapper for Reviews per Listing per Month.

Input (from clean_reviews TSV):
  listing_id\tdate\tyyyy-mm

Emits:
  key:   "<listing_id>|<yyyy-mm>"
  value: 1

Using a compound key keeps Hadoop Streaming defaults simple (no custom
partitioner or key field count needed).
"""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if not parts:
        continue
    try:
        lid = (parts[0] or "").strip()
        if not lid:
            continue
        # Prefer the precomputed yyyy-mm field if present, else derive from date
        ym = (parts[2] if len(parts) > 2 else "").strip()
        if not ym and len(parts) > 1:
            d = (parts[1] or "").strip()
            ym = d[:7] if len(d) >= 7 else ""
        if not ym:
            continue
        print(f"{lid}|{ym}\t1")
    except Exception:
        # Skip malformed lines
        continue

