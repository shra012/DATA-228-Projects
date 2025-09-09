#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

COUNTRY="${1:-$COUNTRY_DEFAULT}"
STATE="${2:-$STATE_DEFAULT}"
COUNTY="${3:-$COUNTY_DEFAULT}"
DATE="${4:-$DATE_DEFAULT}"

# Local staging dir
LOCAL_DIR="${DATA_DIR}/${COUNTRY}/${STATE}/${COUNTY}/${DATE}"
mkdir -p "${LOCAL_DIR}"

BASE_URL="https://data.insideairbnb.com/${COUNTRY}/${STATE}/${COUNTY}/${DATE}/data"
FILES=("listings.csv.gz" "reviews.csv.gz" "calendar.csv.gz")

echo "==> Fetching ${COUNTRY}/${STATE}/${COUNTY}/${DATE}"
for f in "${FILES[@]}"; do
  echo "    - ${f}"
  curl -fSLo "${LOCAL_DIR}/${f}" "${BASE_URL}/${f}"
done

echo "==> Putting to HDFS: ${HDFS_RAW}/{listings,reviews,calendar}"
hdfs dfs -mkdir -p "${HDFS_RAW}"/{listings,reviews,calendar}
hdfs dfs -put -f "${LOCAL_DIR}/listings.csv.gz" "${HDFS_RAW}/listings/"
hdfs dfs -put -f "${LOCAL_DIR}/reviews.csv.gz" "${HDFS_RAW}/reviews/"
hdfs dfs -put -f "${LOCAL_DIR}/calendar.csv.gz" "${HDFS_RAW}/calendar/"

echo "Done. HDFS raw now has compressed inputs."
