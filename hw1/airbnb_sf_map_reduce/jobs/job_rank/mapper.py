#!/usr/bin/env python3
# Input lines: "neighbourhood \t count"
# Output key: sortable key for DESC count   value: "count\tneighbourhood"
import sys
MAX = 10_000_000_000  # 10 digits
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        nb, c = line.split("\t")
        c = int(c)
        inv = MAX - c
        sortkey = f"{inv:010d}"
        print(f"{sortkey}\t{c}\t{nb}")
    except:
        continue
