#!/usr/bin/env python3
# Input TSV (11 cols)
# Output key: nb \t room_type   value: price \t 1
import sys
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 11:
        continue
    _id, nb, rt, price, mn, nor, baths, bedr, beds, a30, a365 = parts
    try:
        p = float(price)
        print(f"{nb}\t{rt}\t{p}\t1")
    except:
        continue
