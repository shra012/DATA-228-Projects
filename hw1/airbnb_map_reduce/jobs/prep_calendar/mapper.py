#!/usr/bin/env python3
"""
Preprocess calendar.csv rows into a compact TSV:

Input (CSV): listing_id, date, available, price, ...
Output (TSV): listing_id\tdate\tis_available(0/1)\tprice_float_or_blank
"""
import sys, csv, re

PRICE_RX = re.compile(r"[\d.,]+")

def to_float_price(v):
    if not v:
        return None
    m = PRICE_RX.search(v)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except:
        return None

def to_bool01(v):
    s = (v or "").strip().lower()
    if s in ("t", "true", "1", "yes", "y"): return 1
    if s in ("f", "false", "0", "no", "n"): return 0
    return 0

reader = csv.DictReader(sys.stdin)
for row in reader:
    try:
        lid = (row.get("listing_id") or "").strip()
        if not lid:
            continue
        date = (row.get("date") or "").strip()
        avail = to_bool01(row.get("available"))
        price = to_float_price(row.get("price"))
        print(f"{lid}\t{date}\t{avail}\t{'' if price is None else f'{price:.2f}'}")
    except Exception:
        continue

