#!/usr/bin/env python3
"""
Reducer for listings ⨝ reviews on listing_id.
Accumulates review count per listing and emits neighbourhood-scoped counts.

Input values: "L\t<nb>" and "R\t1"
Output: "<neighbourhood>\t1" (one line per review)
  - downstream aggregator sums these to get total reviews per neighbourhood
"""
import sys

current = None
nb = None
rcount = 0

def flush(nb, rcount):
    if not nb or rcount <= 0:
        return
    # Emit one line per review to allow a simple downstream sum
    for _ in range(rcount):
        print(f"{nb}\t1")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue
    lid, tag = parts[0], parts[1]
    if lid != current:
        flush(nb, rcount)
        current, nb, rcount = lid, None, 0

    if tag == 'L':
        nb = parts[2]
    elif tag == 'R':
        try:
            rcount += int(parts[2])
        except:
            continue

flush(nb, rcount)

