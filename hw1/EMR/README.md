# Airbnb EMR Insights

PySpark job for Santa Clara County Inside Airbnb data. Ingests listings, calendar, reviews, and neighbourhood lookups, enriches them, and publishes ten metric tables for revenue, demand, compliance, and pricing decisions. Run locally for development or submit to EMR for production-scale runs.

## Quick Start
- Drop the raw snapshot at `data/raw/santa_clara_county/$DATASET_DATE/` (`listings.csv[.gz]`, `calendar.csv[.gz]`, `reviews.csv[.gz]`, `neighbourhoods.csv`).
- Configure AWS CLI and update bucket, region, and key-pair values in `run_emr_job.sh`.
- Use `.venv/bin/python` for local tooling; `run_emr_job.sh` handles bundling, uploads, and EMR submission.

## Commands
Local Spark run
```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) PYSPARK_PYTHON=.venv/bin/python spark-submit
    --master local[*] airbnb_insights_job.py \
    --listings data/raw/santa_clara_county/2025-06-23/listings.csv \
    --calendar data/raw/santa_clara_county/2025-06-23/calendar.csv \
    --reviews data/raw/santa_clara_county/2025-06-23/reviews.csv \
    --neighbourhoods data/raw/santa_clara_county/2025-06-23/neighbourhoods.csv \
    --output results --output-format parquet --coalesce 1
```
Launch EMR job `./run_emr_job.sh` </br>
Monitor EMR job `./run_emr_job.sh monitor` </br>
Pull results `aws s3 sync s3://$S3_BUCKET_DATA/airbnb/metrics/santa_clara_county/$DATASET_DATE/ results/`

## Outputs
Each metric is written to its own folder (Parquet by default):

- `neighbourhood_investment`: RevPAR, booking rate, demand index by neighbourhood + month.
- `dynamic_pricing`: Nightly price percentiles and booking rate by neighbourhood, room type, and month.
- `minimum_stay`: Occupancy and revenue by minimum-night buckets.
- `amenity_roi`: Occupancy and revenue lift for key amenities.
- `seasonality`: Seasonal index at market and neighbourhood-group levels.
- `host_performance`: Host-level occupancy, revenue, reviews, and support flags.
- `portfolio_mix`: RevPAR and occupancy by property/room type.
- `supply_saturation`: Listing growth vs demand trend by neighbourhood.
- `compliance_exposure`: License, stay-limit, and fee risk indicators per listing.
- `fee_optimization`: Base vs adjusted revenue, fee share, and effective rate on booked nights.

## Project Layout
```
.
├── airbnb/              
├── airbnb_insights_job.py
├── run_emr_job.sh
├── emr_job_config.json
└── data/raw/santa_clara_county/$DATASET_DATE/
    ├── listings.csv
    ├── calendar.csv
    ├── reviews.csv
    └── neighbourhoods.csv
```

## Notes
- Default EMR cluster: EMR 7.10, one `m5.large` master, two `m5.large` cores, auto-terminates on completion.
- Tweak `configs/bootstrap.sh` if extra system packages are required.
