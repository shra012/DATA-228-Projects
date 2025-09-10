# Airbnb SF MapReduce Project

## 📌 Overview
This project implements a small **MapReduce data pipeline** on Airbnb datasets using **Hadoop Streaming** with Python mappers and reducers.  
It demonstrates how to:
- **Ingest InsideAirbnb data** into HDFS
- **Preprocess (clean) raw datasets**
- **Compute aggregate metrics** (average price per neighborhood & room type)
- **Evaluate budget supply and rank neighborhoods**

The project is designed for **step-by-step execution** via `make`, and can process **listings, reviews, calendar, or all raw data**.

---

## 📂 Project Organization

```
airbnb_sf_map_reduce/
├─ bin/                     # Executable scripts
│  ├─ env.sh                # Shared environment variables (HDFS paths, dirs)
│  ├─ hdfs_init.sh          # Initializes HDFS namespace
│  ├─ fetch_and_put.sh      # Downloads InsideAirbnb data and uploads to HDFS
│  ├─ run.sh                # Runs a single job (data_prep, avg, budget, rank)
│  └─ run_all.sh            # Runs the full pipeline end-to-end
├─ jobs/                    # Hadoop Streaming jobs (mapper + reducer)
│  ├─ data_prep/            # Cleans raw input (listings/reviews/calendar)
│  ├─ avg_by_nb_rt/         # Computes average price per neighborhood × room type
│  ├─ budget_supply/        # Identifies affordable supply
│  └─ budget_supply_ranked/ # Ranks budget supply results
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
/projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce
```

Subdirectories:
- `raw/` → contains uploaded InsideAirbnb files  
  - `raw/listings/`
  - `raw/reviews/`
  - `raw/calendar/`
- `clean/` → cleaned dataset (from `data_prep`)
- `avg_by_nb_rt/` → average price per neighborhood × room type
- `budget_supply/` → supply of budget listings
- `budget_supply_ranked/` → ranking of budget supply results

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

3. **Run preprocessing (data_prep)**
   ```bash
   make data_prep RAW=listings   # or reviews | calendar | all
   ```
   Cleans raw input and writes to `clean/`.

4. **Run analytics jobs**
   ```bash
   make avg     # computes average price by neighborhood & room type
   make budget  # calculates budget supply
   make rank    # ranks neighborhoods by budget supply
   ```

5. **Full pipeline**
   ```bash
   make all RAW=listings
   ```
   Runs everything in the right order.

---

## 📊 Outputs

- **Cleaned Data (`clean/`)**  
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
| `make data_prep`    | Clean raw data (choose RAW=listings\|reviews\|calendar\|all)             |
| `make avg`          | Compute average price per neighborhood × room type                          |
| `make budget`       | Identify budget supply                                                      |
| `make rank`         | Rank budget supply                                                          |
| `make all`          | Run the full pipeline (init → fetch → data_prep → avg → budget → rank)      |
| `make quick`        | Shortcut: data_prep + avg                                                   |
| `make clean_outputs`| Remove MR output dirs so jobs can be re-run without HDFS conflicts          |

---

## ✅ What This Project Achieves

- **End-to-end MapReduce pipeline**: from raw CSV ingestion → cleaning → analytics → ranking.  
- **Reproducible workflow**: via `make` targets and standardized HDFS layout.  
- **Flexible inputs**: you can process *listings*, *reviews*, *calendar*, or *all* raw files.  
- **Extensible design**: adding new jobs is as simple as dropping mapper/reducer scripts under `jobs/` and wiring them in `run.sh`/`run_all.sh`.

