#!/usr/bin/env python3
"""
Sum two integer fields by key.

Input: key\ta\tb
Output: key\tSUM(a)\tSUM(b)
"""
import sys

current = None
sum_a = 0
sum_b = 0

def flush(k, a, b):
    if k is not None:
        print(f"{k}\t{a}\t{b}")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 3:
        continue
    k, a, b = parts
    try:
        a = int(a); b = int(b)
    except:
        continue
    if k != current:
        flush(current, sum_a, sum_b)
        current, sum_a, sum_b = k, a, b
    else:
        sum_a += a
        sum_b += b
flush(current, sum_a, sum_b)

