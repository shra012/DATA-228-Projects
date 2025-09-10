#!/usr/bin/env python3
"""
Mapper for joining cleaned listings (11-col TSV) with per-listing calendar rollups (3-col TSV).

Emits:
  key: listing_id
  value: "L\t<neighbourhood>" for listing rows
         "C\t<booked_days>\t<total_days>" for calendar rollup rows
"""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if not parts:
        continue
    try:
        if len(parts) == 11:
            lid, nb = parts[0], parts[1]
            if lid and nb:
                print(f"{lid}\tL\t{nb}")
        elif len(parts) == 3:
            lid, booked, total = parts
            if lid:
                print(f"{lid}\tC\t{booked}\t{total}")
    except Exception:
        continue

