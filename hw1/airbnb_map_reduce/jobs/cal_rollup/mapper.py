#!/usr/bin/env python3
"""
Roll up cleaned calendar rows to per-listing day counts.

Input (TSV): listing_id\tdate\tis_available\tprice
Output: listing_id\tbooked_days_increment\ttotal_days_increment
  - booked_days_increment = 1 if available == 0, else 0
  - total_days_increment = 1 for every row
"""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue
    lid, date, avail = parts[0], parts[1], parts[2]
    if not lid:
        continue
    try:
        a = int(avail)
    except:
        a = 0
    booked = 1 if a == 0 else 0
    print(f"{lid}\t{booked}\t1")

