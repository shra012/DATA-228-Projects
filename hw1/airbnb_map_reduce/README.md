# Airbnb MapReduce Project

## 📌 Overview
This project implements a small **MapReduce data pipeline** on Airbnb datasets using **Hadoop Streaming** with Python mappers and reducers.  
It demonstrates how to:
- **Ingest InsideAirbnb data** into HDFS
- **Preprocess (clean) raw datasets**
- **Compute aggregate metrics** (average price per neighborhood & room type)
- **Evaluate budget supply and rank neighborhoods**

The project is designed for **step-by-step execution** via `make`, and can process **listings, reviews, calendar, or all raw data**. Cross-file analytics (listings ⨝ reviews/calendar) are included.

---

## 📂 Project Organization

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

## 🏗️ HDFS Namespace

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
- `calendar_rollup_per_listing/` → per-listing booked/total day counts
- `occupancy_by_neighbourhood/` → nb-level booked/total day counts

---

## ⚙️ Workflow

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
  make rank    # ranks neighborhoods by budget supply
  ```

5. **Cross-file analytics (new)**
   ```bash
   make reviews_by_nb  # listings ⨝ reviews → total reviews per neighbourhood
   make occupancy      # listings ⨝ calendar → booked/total days per neighbourhood
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

## 📊 Outputs

- **Cleaned Listings (`clean_listings/`)**  
  Raw CSV → normalized and filtered.

- **Average by Neighborhood & Room Type (`avg_by_nb_rt/`)**  
  Example output row:  
  ```
  Mission District, Entire home/apt, 187.35
  ```

- **Budget Supply (`budget_supply/`)**  
  Flags listings under a defined budget threshold.

- **Budget Supply Ranked (`budget_supply_ranked/`)**  
  Produces ranking of neighborhoods by number of budget listings.

---

## 🧰 Make Targets

| Target              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `make init`         | Initialize HDFS directory tree                                              |
| `make fetch`        | Download & upload raw InsideAirbnb data                                     |
| `make data_prep`    | Clean listings only (use `prep_reviews`/`prep_calendar` for others)        |
| `make avg`          | Compute average price per neighborhood × room type                          |
| `make budget`       | Identify budget supply                                                      |
| `make rank`         | Rank budget supply                                                          |
| `make all`          | Run the full pipeline (init → fetch → data_prep → avg → budget → rank)      |
| `make quick`        | Shortcut: data_prep + avg                                                   |
| `make reviews_by_nb`| Listings ⨝ reviews → total reviews per neighbourhood                        |
| `make occupancy`   | Listings ⨝ calendar → booked/total days per neighbourhood                    |
| `make clean_outputs`| Remove MR output dirs so jobs can be re-run without HDFS conflicts         |

---

## ✅ What This Project Achieves

- **End-to-end MapReduce pipeline**: from raw CSV ingestion → cleaning → analytics → ranking.  
- **Reproducible workflow**: via `make` targets and standardized HDFS layout.  
- **Flexible inputs**: you can process *listings*, *reviews*, *calendar*, or *all* raw files.  
- **Extensible design**: adding new jobs is as simple as dropping mapper/reducer scripts under `jobs/` and wiring them in `run.sh`/`run_all.sh`.
