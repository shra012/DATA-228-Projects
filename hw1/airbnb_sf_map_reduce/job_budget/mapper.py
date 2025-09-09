#!/usr/bin/env python3
# Input TSV (11 cols)
# Output key: nb    value: 1 (only if budget & short-stay)
import sys
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 11:
        continue
    _id, nb, rt, price, mn, nor, baths, bedr, beds, a30, a365 = parts
    try:
        p = float(price); mn_i = int(mn)
        if p <= 150.0 and mn_i <= 7:
            print(f"{nb}\t1")
    except:
        continue
