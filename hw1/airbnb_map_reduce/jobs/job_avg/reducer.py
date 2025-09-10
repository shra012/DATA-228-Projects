#!/usr/bin/env python3
# Aggregates sum(price), count -> avg
import sys

current = None
sum_p, cnt = 0.0, 0

def flush(k, s, c):
    if not k or c == 0: return
    nb, rt = k
    print(f"{nb}\t{rt}\t{(s/c):.2f}\t{c}")

for line in sys.stdin:
    nb, rt, p, c = line.rstrip("\n").split("\t")
    key = (nb, rt)
    p = float(p); c = int(c)
    if key != current:
        flush(current, sum_p, cnt)
        current, sum_p, cnt = key, p, c
    else:
        sum_p += p; cnt += c
flush(current, sum_p, cnt)
