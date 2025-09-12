# Airbnb MapReduce Project

## Overview
This project implements a small **MapReduce data pipeline** on Airbnb datasets using **Hadoop Streaming** with Python mappers and reducers.  
It demonstrates how to:
- **Ingest InsideAirbnb data** into HDFS
- **Preprocess (clean) raw datasets**
- **Compute aggregate metrics** (average price per neighborhood & room type)
- **Evaluate budget supply and rank neighborhoods**
 - **Join across datasets** to compute review counts and occupancy proxies

The project is designed for **step-by-step execution** via `make`, and can process **listings, reviews, calendar, or all raw data**. Cross-file analytics (listings ⨝ reviews/calendar) are included.

---

## Project Organization

```
airbnb_map_reduce/
├─ bin/                     # Executable scripts
│  ├─ env.sh                # Shared environment variables (HDFS paths, dirs)
│  ├─ hdfs_init.sh          # Initializes HDFS namespace
│  ├─ fetch_and_put.sh      # Downloads InsideAirbnb data and uploads to HDFS
│  ├─ run.sh                # Runs a single job (data_prep, prep_reviews, prep_calendar, avg, budget, rank, joins, aggs)
│  └─ run_all.sh            # Runs the full pipeline end-to-end
├─ jobs/                    # Hadoop Streaming jobs (mapper + reducer)
│  ├─ data_prep/            # Cleans raw listings → normalized TSV (11 cols)
│  ├─ prep_reviews/         # Cleans reviews → (listing_id, date, yyyy-mm)
│  ├─ prep_calendar/        # Cleans calendar → (listing_id, date, avail, price)
│  ├─ job_avg/              # Computes average price per neighborhood × room type
│  ├─ job_budget/           # Identifies affordable supply
│  ├─ job_rank/             # Ranks budget supply results
│  ├─ join_listings_reviews/# Join listings ⨝ reviews → nb-level review counts
│  ├─ cal_rollup/           # Per-listing calendar day rollups (booked/total)
│  ├─ join_listings_calendar# Join listings ⨝ calendar rollups → nb totals
│  ├─ agg_sum/              # Sum integer counts by key
│  └─ agg_two_ints/         # Sum two integer fields by key
├─ data/                    # Local staging area (downloads from InsideAirbnb)
├─ logs/                    # Execution logs (driver + YARN logs if enabled)
├─ config/                  # Optional environment configs
│  ├─ env.example
│  └─ env.local (gitignored)
├─ Makefile                 # Build/run automation
└─ README.md                # Project documentation
```

---

## MapReduce Jobs

These are the runnable job keys (as used by `bin/run.sh`) and their outputs:

- data_prep: cleans listings → `clean_listings/`
- prep_reviews: cleans reviews → `clean_reviews/`
- prep_calendar: cleans calendar → `clean_calendar/`
- avg_by_nb_rt: average price by neighbourhood × room type → `avg_by_nb_rt/`
- budget_supply: budget, short-stay supply per neighbourhood → `budget_supply/`
- budget_supply_ranked: neighbourhoods ranked by budget supply → `budget_supply_ranked/`
- join_listings_reviews: listings ⨝ reviews → intermediate → `reviews_by_neighbourhood.raw/`
- agg_nb_counts: aggregate intermediate review counts → `reviews_by_neighbourhood/`
- reviews_per_listing_month: reviews only → `reviews_per_listing_month/`
- cal_rollup_per_listing: booked/total day counts per listing → `calendar_rollup_per_listing/`
- join_listings_calendar: listings ⨝ calendar rollups → intermediate → `occupancy_by_neighbourhood.raw/`
- agg_occupancy_by_nb: aggregate to booked/total per neighbourhood → `occupancy_by_neighbourhood/`

Tip: You usually invoke the composed Make targets (`make reviews_by_nb`, `make occupancy`) which run the relevant join + aggregation steps in order.

---

## HDFS Namespace

All data is stored under:

```
/projects/DATA-228-Projects/hw1/airbnb_map_reduce
```

Subdirectories:
- `raw/` → contains uploaded InsideAirbnb files  
  - `raw/listings/`
  - `raw/reviews/`
  - `raw/calendar/`
- `clean_listings/` → cleaned listings (from `data_prep`)
- `clean_reviews/` → cleaned reviews (from `prep_reviews`)
- `clean_calendar/` → cleaned calendar (from `prep_calendar`)
- `avg_by_nb_rt/` → average price per neighborhood × room type
- `budget_supply/` → supply of budget listings
- `budget_supply_ranked/` → ranking of budget supply results
- `reviews_by_neighbourhood/` → total reviews per neighbourhood
- `reviews_per_listing_month/` → per-listing monthly review counts
- `calendar_rollup_per_listing/` → per-listing booked/total day counts
- `occupancy_by_neighbourhood/` → nb-level booked/total day counts
  - intermediates: `reviews_by_neighbourhood.raw/`, `occupancy_by_neighbourhood.raw/`

---

## Workflow

1. **Initialize HDFS tree**
   ```bash
   make init
   ```
   Creates the required HDFS directory structure.

2. **Fetch InsideAirbnb data**
   ```bash
   make fetch
   ```
   Downloads CSV.gz files from InsideAirbnb and puts them into HDFS `raw/`.
   If you already have data under `airbnb_map_reduce/data/`, you can upload manually with:
   ```bash
   # example
   hdfs dfs -mkdir -p $(source airbnb_map_reduce/bin/env.sh; echo $HDFS_RAW/{listings,reviews,calendar})
   hdfs dfs -put -f airbnb_map_reduce/data/**/listings*.csv* $(source airbnb_map_reduce/bin/env.sh; echo $HDFS_RAW)/listings/
   hdfs dfs -put -f airbnb_map_reduce/data/**/reviews*.csv*  $(source airbnb_map_reduce/bin/env.sh; echo $HDFS_RAW)/reviews/
   hdfs dfs -put -f airbnb_map_reduce/data/**/calendar*.csv* $(source airbnb_map_reduce/bin/env.sh; echo $HDFS_RAW)/calendar/
   ```

3. **Run preprocessing**
   ```bash
   make data_prep RAW=listings   # cleans listings → clean_listings/
   make prep_reviews             # cleans reviews  → clean_reviews/
   make prep_calendar            # cleans calendar → clean_calendar/
   ```
   Note: `data_prep` is for listings. Use `prep_reviews` and `prep_calendar` for the other raw files.

4. **Run analytics jobs**
    ```bash
    make avg     # computes average price by neighborhood & room type
    make budget  # calculates budget supply
    make rank    # ranks neighborhoods budget supply
    ``` 

5. **Cross-file analytics**
    ```bash
    make reviews_by_nb  # listings ⨝ reviews → total reviews per neighbourhood
    make occupancy      # listings ⨝ calendar → booked/total days per neighbourhood
    ```
    Under the hood these run:
    ```bash
    # Reviews by neighbourhood
    bash airbnb_map_reduce/bin/run.sh join_listings_reviews
    bash airbnb_map_reduce/bin/run.sh agg_nb_counts

    # Occupancy by neighbourhood
    bash airbnb_map_reduce/bin/run.sh cal_rollup_per_listing
    bash airbnb_map_reduce/bin/run.sh join_listings_calendar
    bash airbnb_map_reduce/bin/run.sh agg_occupancy_by_nb
    ```

6. **Full pipeline**
   ```bash
   # RAW controls which dataset pipeline runs
   make all RAW=listings   # listings: data_prep → avg → budget → rank
   make all RAW=reviews    # reviews: data_prep + prep_reviews → reviews_by_nb (needs listings for join)
   make all RAW=calendar   # calendar: data_prep + prep_calendar → occupancy (needs listings for join)
   make all RAW=all        # listings+reviews+calendar: preprocess + listings analytics + cross-file analytics
   ```
   Each variant runs its corresponding steps in order. For `RAW=all`, it also runs:
   - listings ⨝ reviews → total reviews per neighbourhood
   - listings ⨝ calendar → booked/total days per neighbourhood

   Alternative: run everything end-to-end
   ```bash
   bash airbnb_map_reduce/bin/run_all.sh
   ```
   This initializes HDFS, preprocesses listings/reviews/calendar, runs analytics, and prints quick previews.
   It assumes raw data is already present in HDFS under `${HDFS_RAW}/{listings,reviews,calendar}`. Use `make fetch` or manually `hdfs dfs -put` from `airbnb_map_reduce/data` beforehand.

   Note: `make all` intentionally does not run `init` or `fetch`. Run them once beforehand, or use `make check RAW=...` to verify required HDFS raw inputs exist before running the pipeline.

---

## Outputs

- **Cleaned Listings (`clean_listings/`)**: normalized TSV with 11 columns  
  Schema: `listing_id  neighbourhood  room_type  price  min_nights  num_reviews  baths  bedrooms  beds  avail_30  avail_365`  
  Example:  
  ```
  12345678	mission district	Entire home/apt	187.35	2	57	1.0	1.0	1.0	10	120
  ```

- **Cleaned Reviews (`clean_reviews/`)**: compact TSV for joins  
  Schema: `listing_id  date  yyyy-mm`  
  Example:  
  ```
  12345678	2023-09-10	2023-09
  ```

- **Cleaned Calendar (`clean_calendar/`)**: compact TSV for joins and rollups  
  Schema: `listing_id  date  is_available(0/1)  price_or_blank`  
  Example:  
  ```
  12345678	2023-09-10	0	175.00
  ```

- **Average by Neighborhood & Room Type (`avg_by_nb_rt/`)**: average price and count  
  Schema: `neighbourhood  room_type  avg_price  count`  
  Example:  
  ```
  mission district	Entire home/apt	187.35	42
  ```

- **Budget Supply (`budget_supply/`)**: count of budget, short-stay listings per neighbourhood  
  Budget rule: `price <= 150` and `min_nights <= 7`  
  Schema: `neighbourhood  count`  
  Example:  
  ```
  mission district	421
  ```

- **Budget Supply Ranked (`budget_supply_ranked/`)**: neighbourhoods sorted by budget supply (desc)  
  Schema: `neighbourhood  count`  
  Example:  
  ```
  mission district	421
  ```

- **Reviews by Neighbourhood (`reviews_by_neighbourhood/`)**: total review events per neighbourhood  
  Pipeline: clean_listings ⨝ clean_reviews → sum  
  Schema: `neighbourhood  total_reviews`  
  Example:  
  ```
  mission district	1245
  ```

- **Reviews per Listing per Month (`reviews_per_listing_month/`)**: counts of review events grouped by listing and year-month  
  Pipeline: from clean_reviews only  
  Schema: `listing_id  yyyy-mm  total_reviews`  
  Example:  
  ```
  12345678	2023-09	3
  ```

- **Calendar Rollup Per Listing (`calendar_rollup_per_listing/`)**: booked/total days per listing  
  Pipeline: from clean_calendar → per-listing rollup  
  Schema: `listing_id  booked_days  total_days`  
  Example:  
  ```
  12345678	183	365
  ```

- **Occupancy by Neighbourhood (`occupancy_by_neighbourhood/`)**: booked/total days per neighbourhood  
  Pipeline: clean_calendar → per-listing rollup ⨝ clean_listings → sum  
  Schema: `neighbourhood  booked_days  total_days`  
  Example:  
  ```
  mission district	305412	512340
  ```

---

## Make Targets

| Target              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `make init`         | Initialize HDFS directory tree                                              |
| `make fetch`        | Download & upload raw InsideAirbnb data                                     |
| `make check`        | Verify required HDFS raw inputs exist for the chosen RAW                    |
| `make data_prep`    | Clean listings only (use `prep_reviews`/`prep_calendar` for others)        |
| `make avg`          | Compute average price per neighborhood × room type                          |
| `make budget`       | Identify budget supply                                                      |
| `make rank`         | Rank budget supply                                                          |
| `make all`          | Run the pipeline (preflight only; no init/fetch)                             |
| `make quick`        | Shortcut: data_prep + avg                                                   |
| `make reviews_by_nb`| Listings ⨝ reviews → total reviews per neighbourhood                        |
| `make reviews_per_listing_month` | Reviews → per listing per month counts                         |
| `make occupancy`    | Listings ⨝ calendar → booked/total days per neighbourhood                   |
| `make clean_outputs`| Remove MR output dirs so jobs can be re-run without HDFS conflicts         |

---

## What This Project Achieves

- **End-to-end MapReduce pipeline**: from raw CSV ingestion → cleaning → analytics → ranking.  
- **Reproducible workflow**: via `make` targets and standardized HDFS layout.  
- **Flexible inputs**: you can process *listings*, *reviews*, *calendar*, or *all* raw files.  
- **Extensible design**: adding new jobs is as simple as dropping mapper/reducer scripts under `jobs/` and wiring them in `run.sh`/`run_all.sh`.
