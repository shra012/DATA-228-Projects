#!/usr/bin/env python3
"""
Reducer: Reviews per Listing per Month (balanced join)

Input (sorted by key fields listing_id, yyyy-mm; partitioned by listing_id):
  keyfields: lid, ym
  value: one of
    - L  <neighbourhood>  <room_type>
    - R  <yyyy-mm>        <review_id>

Emits for each review row:
  listing_id  neighbourhood  room_type  yyyy-mm  review_id

Assumes listing metadata (L) appears before monthly rows due to
sentinel month in mapper and lexicographic ordering.
"""
import sys

current_lid = None
nb = ""
rt = ""

def flush_reviews(lid, nb, rt, rows):
    if not lid:
        return
    for ym, rid in rows:
        print(f"{lid}\t{nb}\t{rt}\t{ym}\t{rid}")

buffer = []  # list of (ym, rid)

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue
    # key: lid, ym
    lid = parts[0]
    # ym_key = parts[1]  # not required directly
    tag = parts[2]

    if lid != current_lid:
        flush_reviews(current_lid, nb, rt, buffer)
        current_lid, nb, rt, buffer = lid, "", "", []

    if tag == 'L':
        # L  nb  rt
        nb = parts[3] if len(parts) > 3 else nb
        rt = parts[4] if len(parts) > 4 else rt
    elif tag == 'R':
        # R  ym  rid
        ym = parts[3] if len(parts) > 3 else ""
        rid = parts[4] if len(parts) > 4 else ""
        if ym:
            buffer.append((ym, rid))

flush_reviews(current_lid, nb, rt, buffer)

