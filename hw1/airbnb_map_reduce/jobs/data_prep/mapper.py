#!/usr/bin/env python3
import sys, csv, re

# robust price parser like: "$1,234.00"
PRICE_RX = re.compile(r"[\d.,]+")

def to_float_price(v):
    if not v: return None
    m = PRICE_RX.search(v)
    if not m: return None
    return float(m.group(0).replace(",", ""))

def to_int(v, default=0):
    try: return int(float(v))  # some fields come as "1.0"
    except: return default

def to_float(v, default=None):
    try: return float(v)
    except: return default

reader = csv.DictReader(sys.stdin)
for row in reader:
    try:
        _id   = row.get("id")
        nb    = (row.get("neighbourhood_cleansed") or "").strip().lower()
        rt    = (row.get("room_type") or "").strip()
        price = to_float_price(row.get("price"))
        mn    = to_int(row.get("minimum_nights"), 0)
        nor   = to_int(row.get("number_of_reviews"), 0)
        baths = to_float(row.get("bathrooms"), None) or to_float((row.get("bathrooms_text") or "").split()[0], None)
        beds  = to_float(row.get("beds"), None)
        bedr  = to_float(row.get("bedrooms"), None)
        a30   = to_int(row.get("availability_30"), 0)
        a365  = to_int(row.get("availability_365"), 0)

        # minimal validation & outlier clipping for cleaner downstream stats
        if not (_id and nb and rt and price and price > 0 and price < 5000):
            continue
        if mn < 0:  # invalid minimum nights
            continue

        # print TSV (order matters for following jobs)
        # id  nb  rt  price  min_nights  num_reviews  baths  bedrooms  beds  avail30  avail365
        print(f"{_id}\t{nb}\t{rt}\t{price:.2f}\t{mn}\t{nor}\t"
              f"{'' if baths is None else baths}\t"
              f"{'' if bedr is None else bedr}\t"
              f"{'' if beds is None else beds}\t{a30}\t{a365}")
    except Exception:
        # skip any malformed rows quietly
        continue
