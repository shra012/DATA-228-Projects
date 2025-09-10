#!/usr/bin/env python3
"""
Mapper for joining cleaned listings (11-col TSV) with cleaned reviews (3-col TSV).

Emits:
  key: listing_id
  value: "L\t<neighbourhood>" for listing rows
         "R\t1" for review rows (one per review instance)
"""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if not parts:
        continue
    try:
        if len(parts) == 11:
            # listings clean TSV
            lid, nb = parts[0], parts[1]
            if lid and nb:
                print(f"{lid}\tL\t{nb}")
        elif len(parts) >= 1:
            # reviews clean TSV (listing_id [\t date \t ym])
            lid = parts[0]
            if lid:
                print(f"{lid}\tR\t1")
    except Exception:
        continue

