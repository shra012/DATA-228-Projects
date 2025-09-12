#!/usr/bin/env python3
"""
Reducer for Reviews per Listing per Month (reduce-side join).

Input values per listing_id:
  L  <neighbourhood>  <room_type>
  R  <yyyy-mm>        <review_id>

Output (sorted by yyyy-mm for each listing_id):
  listing_id  neighbourhood  room_type  yyyy-mm  review_id
"""
import sys

current = None
nb = ""
rt = ""
reviews = [] 

def flush(lid, nb, rt, reviews):
    if not lid:
        return
 
    reviews.sort(key=lambda x: (x[0], x[1]))
    for ym, rid in reviews:
        print(f"{lid}\t{nb}\t{rt}\t{ym}\t{rid}")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue
    lid, tag = parts[0], parts[1]

    if lid != current:
        flush(current, nb, rt, reviews)
        current, nb, rt, reviews = lid, "", "", []

    if tag == 'L':
        # L  nb  rt
        nb = parts[2] if len(parts) > 2 else nb
        rt = parts[3] if len(parts) > 3 else rt
    elif tag == 'R':
        # R  ym  rid
        ym = parts[2] if len(parts) > 2 else ""
        rid = parts[3] if len(parts) > 3 else ""
        if ym:
            reviews.append((ym, rid))

flush(current, nb, rt, reviews)
