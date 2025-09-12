#!/usr/bin/env python3
"""
Mapper: Reviews per Listing per Month (balanced join)

Goal:
- Emit a two-field key so Hadoop can partition by listing_id while keeping
  month ordering (secondary sort) within each listing.

Inputs (mixed):
- Cleaned listings TSV (11 cols): listing_id, neighbourhood, room_type, ...
- Cleaned reviews TSV (4 cols):   listing_id, review_id, date, yyyy-mm

Outputs:
- Key fields: listing_id, yyyy-mm (for listings we use "0000-00" as a sentinel
  that sorts before real months)
- Values:
  - Listings: "L\t<neighbourhood>\t<room_type>"
  - Reviews:  "R\t<yyyy-mm>\t<review_id>"

Use with KeyFieldBasedPartitioner (-k1,1) so all keys for a listing go
to the same reducer while remaining sorted by month.
"""
import sys

SENTINEL_MONTH = "0000-00"

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
            if lid:
                # Two-field key: lid, SENTINEL_MONTH (sorts first)
                print(f"{lid}\t{SENTINEL_MONTH}\tL\t{nb}\t{rt}")
        elif len(parts) >= 4:
            # reviews clean TSV: listing_id, review_id, date, yyyy-mm
            lid = (parts[0] or "").strip()
            rid = (parts[1] or "").strip()
            ym  = (parts[3] or "").strip()
            if lid and ym:
                print(f"{lid}\t{ym}\tR\t{ym}\t{rid}")
        else:
            # Back-compat (older 3-col reviews): listing_id, date, yyyy-mm
            lid = (parts[0] or "").strip()
            ym = (parts[2] if len(parts) > 2 else "").strip()
            if lid and ym:
                print(f"{lid}\t{ym}\tR\t{ym}\t")
    except Exception:
        continue

