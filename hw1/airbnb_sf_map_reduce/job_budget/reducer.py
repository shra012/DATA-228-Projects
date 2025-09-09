#!/usr/bin/env python3
# Counts per neighbourhood
import sys
current, count = None, 0

def flush(k, c):
    if k is not None:
        print(f"{k}\t{c}")

for line in sys.stdin:
    nb, one = line.rstrip("\n").split("\t")
    if nb != current:
        flush(current, count)
        current, count = nb, 1
    else:
        count += 1
flush(current, count)
