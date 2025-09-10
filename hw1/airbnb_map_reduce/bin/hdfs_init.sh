#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

echo "==> Ensuring HDFS base tree exists at ${HDFS_NS}"
hdfs dfs -mkdir -p "${HDFS_RAW}"/{listings,reviews,calendar}
hdfs dfs -mkdir -p "${HDFS_CLEAN_listings}" \
  "${HDFS_AVG}" \
  "${HDFS_BUDGET}" \
  "${HDFS_BUDGET_RANK}" \
  "${HDFS_CLEAN_reviews}" \
  "${HDFS_CLEAN_calendar}" \
  "${HDFS_REVIEWS_BY_NB}" \
  "${HDFS_CAL_ROLLUP_PER_LISTING}" \
  "${HDFS_OCCUPANCY_BY_NB}"

echo "HDFS ready."
