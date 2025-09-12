#!/usr/bin/env python3
"""
Mapper for Reviews per Listing per Month (reduce-side join).

Accepts mixed inputs:
  - Cleaned listings TSV (11 cols): listing_id, neighbourhood, room_type, ...
  - Cleaned reviews TSV (4 cols):   listing_id, review_id, date, yyyy-mm

Emits by listing_id with a tag so the reducer can join:
  L records: "<lid>\tL\t<neighbourhood>\t<room_type>"
  R records: "<lid>\tR\t<yyyy-mm>\t<review_id>"
"""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if not parts:
        continue
    try:
        if len(parts) == 11:
            # listings clean TSV
            lid = (parts[0] or "").strip()
            nb  = (parts[1] or "").strip()
            rt  = (parts[2] or "").strip()
            if lid and (nb or rt):
                print(f"{lid}\tL\t{nb}\t{rt}")
        elif len(parts) >= 4:
            # reviews clean TSV (listing_id, review_id, date, yyyy-mm)
            lid = (parts[0] or "").strip()
            rid = (parts[1] or "").strip()
            ym  = (parts[3] or "").strip()
            if lid and ym:
                print(f"{lid}\tR\t{ym}\t{rid}")
        else:
            # Back-compat: prior 3-col reviews (listing_id, date, yyyy-mm)
            lid = (parts[0] or "").strip()
            ym = (parts[2] if len(parts) > 2 else "").strip()
            if lid and ym:
                print(f"{lid}\tR\t{ym}\t")
    except Exception:
        continue
