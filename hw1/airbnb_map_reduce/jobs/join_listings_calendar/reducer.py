#!/usr/bin/env python3
"""
Reducer for listings ⨝ calendar-rollup on listing_id.
Emits neighbourhood-scoped booked/total day counts.

Input values: "L\t<nb>" and "C\t<booked>\t<total>"
Output: "<nb>\t<booked>\t<total>"
"""
import sys

current = None
nb = None
booked = 0
total = 0

def flush(nb, booked, total):
    if not nb or total <= 0:
        return
    print(f"{nb}\t{booked}\t{total}")

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue
    lid, tag = parts[0], parts[1]
    if lid != current:
        flush(nb, booked, total)
        current, nb, booked, total = lid, None, 0, 0

    if tag == 'L':
        nb = parts[2]
    elif tag == 'C':
        if len(parts) >= 4:
            try:
                booked += int(parts[2])
                total += int(parts[3])
            except:
                continue

flush(nb, booked, total)

