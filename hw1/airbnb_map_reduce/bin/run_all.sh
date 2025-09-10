#!/usr/bin/env bash
set -euo pipefail

# Always resolve to repo root, then load env
cd "$(dirname "$0")"
source "./env.sh"
cd "${PROJ_ROOT}"

mkdir -p "${LOGS_DIR}"

log() { echo "[$(date +'%F %T')] $*"; }

# 0) Initialize HDFS base tree
log "Initializing HDFS namespace at ${HDFS_NS}"
bash bin/hdfs_init.sh

log "Assuming raw data already present under ${HDFS_RAW} (listings/reviews/calendar)."

# 1) Preprocessing (cleaned datasets)
log "Running preprocessing jobs (listings, reviews, calendar)"
bash bin/run.sh data_prep --input=listings
bash bin/run.sh prep_reviews
bash bin/run.sh prep_calendar

# 2) Analytics on listings
log "Running listings analytics (avg, budget, rank)"
bash bin/run.sh avg_by_nb_rt
bash bin/run.sh budget_supply
bash bin/run.sh budget_supply_ranked

# 3) Cross-file analytics (listings ⨝ reviews; listings ⨝ calendar)
log "Running cross-file analytics (reviews_by_nb, occupancy_by_nb)"
bash bin/run.sh join_listings_reviews
bash bin/run.sh agg_nb_counts
bash bin/run.sh cal_rollup_per_listing
bash bin/run.sh join_listings_calendar
bash bin/run.sh agg_occupancy_by_nb

# 4) Quick previews
echo "[CLEAN LISTINGS]"
(hdfs dfs -cat "${HDFS_CLEAN_listings}/part-*" 2>/dev/null | head -n 8) || true
echo "[CLEAN REVIEWS]"
(hdfs dfs -cat "${HDFS_CLEAN_reviews}/part-*" 2>/dev/null | head -n 8) || true
echo "[CLEAN CALENDAR]"
(hdfs dfs -cat "${HDFS_CLEAN_calendar}/part-*" 2>/dev/null | head -n 8) || true
echo "[AVG]"
(hdfs dfs -cat "${HDFS_AVG}/part-*" 2>/dev/null | head -n 8) || true
echo "[BUDGET]"
(hdfs dfs -cat "${HDFS_BUDGET}/part-*" 2>/dev/null | head -n 8) || true
echo "[RANK]"
(hdfs dfs -cat "${HDFS_BUDGET_RANK}/part-*" 2>/dev/null | head -n 8) || true
echo "[REVIEWS_BY_NB]"
(hdfs dfs -cat "${HDFS_REVIEWS_BY_NB}/part-*" 2>/dev/null | head -n 8) || true
echo "[CAL_ROLLUP_PER_LISTING]"
(hdfs dfs -cat "${HDFS_CAL_ROLLUP_PER_LISTING}/part-*" 2>/dev/null | head -n 8) || true
echo "[OCCUPANCY_BY_NB]"
(hdfs dfs -cat "${HDFS_OCCUPANCY_BY_NB}/part-*" 2>/dev/null | head -n 8) || true

log "All jobs complete. Driver logs live in: ${LOGS_DIR}"
