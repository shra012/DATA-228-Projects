#!/usr/bin/env python3
"""
Reducer for Reviews per Listing per Month.

Input lines from mapper:
  "<listing_id>|<yyyy-mm>\t1"

Output:
  listing_id\tyyyy-mm\ttotal_reviews
"""
import sys

current_key = None
acc = 0

def flush(k, v):
    if not k:
        return
    # Split compound key back to fields
    if "|" in k:
        lid, ym = k.split("|", 1)
    else:
        lid, ym = k, ""
    if not lid or not ym:
        return
    print(f"{lid}\t{ym}\t{v}")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 1)
    if len(parts) != 2:
        continue
    k, v = parts
    try:
        v = int(v)
    except Exception:
        continue

    if k != current_key:
        flush(current_key, acc)
        current_key, acc = k, v
    else:
        acc += v

flush(current_key, acc)

