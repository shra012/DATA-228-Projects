#!/usr/bin/env python3
"""
Sum integer counts by key.

Input: key\tcount
Output: key\tSUM(count)
"""
import sys

current = None
acc = 0

def flush(k, v):
    if k is not None:
        print(f"{k}\t{v}")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 1)
    if len(parts) != 2:
        continue
    k, v = parts
    try:
        v = int(v)
    except:
        continue
    if k != current:
        flush(current, acc)
        current, acc = k, v
    else:
        acc += v
flush(current, acc)

