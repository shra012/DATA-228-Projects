# Airbnb EMR Insights

This project provisions an EMR cluster, stages Inside Airbnb extracts, and runs a PySpark pipeline that generates ten decision-ready metrics for Santa Clara County short-term rentals. The workflow is automated end-to-end via `run_emr_job.sh`, but the PySpark job (`airbnb_insights_job.py`) can also be exercised locally for development.

## Setup
1. **Download Inside Airbnb data** – place the latest Santa Clara County snapshot under `data/raw/santa_clara_county/<YYYY-MM-DD>/` with at least:
   - `listings.csv.gz`
   - `calendar.csv.gz`
   - `reviews.csv.gz`
   - `neighbourhoods.csv`
   - `neighbourhoods.geojson` (optional, useful for BI layers)
2. **Configure AWS access** – run `aws configure`, ensure `EMR_DefaultRole` and `EMR_EC2_DefaultRole` exist, and set `S3_BUCKET_*`, `AWS_REGION`, and `EC2_KEY_PAIR` in `run_emr_job.sh`.
3. **Run the pipeline** – `chmod +x run_emr_job.sh && ./run_emr_job.sh`. The script uploads `configs/bootstrap.sh` as the EMR bootstrap action (customise it for extra packages). Use `./run_emr_job.sh monitor` to follow the cluster, `./run_emr_job.sh results` to list outputs, and `./run_emr_job.sh logs` for CloudWatch tails.
4. **Local dry-run (optional)** – `spark-submit --master local[*] airbnb_insights_job.py --listings data/raw/.../listings.csv.gz --calendar ... --reviews ... --neighbourhoods ... --output ./local-output --output-format parquet`.

## Goals / Business Decisions
- Neighborhood investment focus: pinpoint high-RevPAR areas experiencing sustained demand growth.
- Dynamic pricing strategy: refine nightly rates by season, neighborhood, and property type to maximize yield.
- Optimal minimum-stay policies: balance occupancy and operating costs through data-backed stay-length rules.
- Amenity ROI: prioritize amenity upgrades that demonstrably increase occupancy or average daily rate.
- Seasonality and event planning: anticipate peaks and troughs to schedule promotions and adjust availability.
- Host performance coaching: identify hosts or listings needing quality interventions based on reviews and cancellations.
- Portfolio expansion mix: select property and room types delivering superior returns in each submarket.
- Supply saturation risk: detect neighborhoods where listing growth outpaces demand to avoid overexpansion.
- Compliance and regulatory exposure: flag entire-home listings breaching local stay limits or licensing rules.
- Cleaning and service fee optimization: tune ancillary fees to protect conversion rates while preserving margin.

Each question maps to a metric output emitted by the PySpark job (see **Outputs** below).

## How It’s Achieved
- **Orchestration (`run_emr_job.sh`)** – packages the `airbnb/` helper modules, stages raw datasets to S3, wires CloudWatch logging, and submits the EMR step with appropriate arguments and dependencies.
- **PySpark processing (`airbnb_insights_job.py`)** – reads listings, calendar, reviews, and neighbourhood lookups; builds shared feature views in `airbnb/prep.py`; and computes the ten business-aligned aggregates defined in `airbnb/metrics.py`.
- **Reusable modules (`airbnb/`)** – `io.py` handles resilient CSV ingestion, `utils.py` normalises currency/percentages, `prep.py` constructs enriched DataFrames, and `metrics.py` returns DataFrames keyed by metric name.
- **Configuration management (`emr_job_config.json`)** – mirrors the runner defaults for IaC or notebook-driven submissions.

## Data Outputs
The job writes one directory per metric under `s3://<S3_BUCKET_DATA>/airbnb/metrics/santa_clara_county/<DATASET_DATE>/` (Parquet by default):

| Subdirectory | Business Question | Highlights |
|--------------|------------------|------------|
| `neighbourhood_investment/` | Neighborhood investment focus | Monthly RevPAR, booking rate, fee share, and demand velocity per neighbourhood. |
| `dynamic_pricing/` | Dynamic pricing strategy | Nightly price distributions (p25/p50/p75), booking rate, and price index by neighbourhood/room type/month. |
| `minimum_stay/` | Minimum-stay policy optimisation | Occupancy and revenue benchmarks by minimum-night bucket and room type. |
| `amenity_roi/` | Amenity ROI | Occupancy/revenue lift for high-impact amenities across the portfolio. |
| `seasonality/` | Seasonality & event planning | Seasonal index vs baseline bookings at market and neighbourhood-group levels. |
| `host_performance/` | Host performance coaching | Host-level occupancy, review score, response rate, recent review volume, and support flags. |
| `portfolio_mix/` | Portfolio expansion mix | RevPAR and demand metrics by property + room type combos. |
| `supply_saturation/` | Supply saturation risk | Listing growth vs occupancy trend to flag oversupplied neighbourhoods. |
| `compliance_exposure/` | Compliance exposure | Listings missing licenses, short-term risks, or high fee share with composite risk score. |
| `fee_optimization/` | Cleaning/service fee optimisation | Base vs adjusted revenue, fee ratios, and effective nightly rates on booked nights. |

Download for offline analysis with `aws s3 sync s3://$S3_BUCKET_DATA/$OUTPUT_S3_PREFIX/ ./results/`.

## Monitoring & Logs
- **CloudWatch** – logs are streamed to `/aws/emr/airbnb-insights/*` groups (driver, executor, YARN, MapReduce). Tail interactively via `./run_emr_job.sh logs`.
- **S3 logs** – EMR step, application, and hardware logs reside under `s3://$S3_BUCKET_LOGS/logs/<cluster-id>/`. Use `./run_emr_job.sh dlogs` to pull and decompress locally.

## Repository Layout
```
.
├── airbnb/                   # PySpark helper package (ingestion, prep, metrics)
├── airbnb_insights_job.py    # Main PySpark entrypoint
├── run_emr_job.sh            # Orchestration + EMR lifecycle script
├── emr_job_config.json       # Reference EMR configuration & step args
└── data/raw/...              # Local Inside Airbnb extracts (not committed)
```

## Cost Considerations
The default EMR cluster uses one `m5.large` master and two `m5.large` core nodes running EMR 7.10. Charges accrue for EC2, EMR, CloudWatch, and S3 transfers/storage. The cluster auto-terminates as soon as the PySpark step finishes; monitor job duration and switch to spot instances or smaller node types if appropriate.

## Contributing
Improvements are welcome—ideas include richer data validation, automated data downloads, expanded geospatial outputs, or packaging the job for AWS Step Functions or MWAA. Submit PRs or adapt internally as needed.
