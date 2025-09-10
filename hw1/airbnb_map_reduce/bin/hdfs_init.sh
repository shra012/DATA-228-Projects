#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

echo "==> Ensuring HDFS base tree exists at ${HDFS_NS}"
hdfs dfs -mkdir -p "${HDFS_RAW}"/{listings,reviews,calendar}
hdfs dfs -mkdir -p "${HDFS_CLEAN}" \
  "${HDFS_AVG}" \
  "${HDFS_BUDGET}" \
  "${HDFS_BUDGET_RANK}"

echo "HDFS ready."
