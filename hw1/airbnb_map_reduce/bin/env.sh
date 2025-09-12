#!/usr/bin/env bash
set -eo pipefail

# Resolve repo root relative to this file, unless PROJ_ROOT already set
_THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
_DEFAULT_ROOT="$(cd "${_THIS_DIR}/.." && pwd -P)"
export PROJ_ROOT="${PROJ_ROOT:-${_DEFAULT_ROOT}}"

# HDFS namespace
export HDFS_NS="${HDFS_NS:-/projects/DATA-228-Projects/hw1/airbnb_map_reduce}"
export HDFS_RAW="${HDFS_RAW:-${HDFS_NS}/raw}"
export HDFS_AVG="${HDFS_AVG:-${HDFS_NS}/avg_by_nb_rt}"
export HDFS_BUDGET="${HDFS_BUDGET:-${HDFS_NS}/budget_supply}"
export HDFS_BUDGET_RANK="${HDFS_BUDGET_RANK:-${HDFS_NS}/budget_supply_ranked}"

# Cleaned datasets
export HDFS_CLEAN_listings="${HDFS_CLEAN_listings:-${HDFS_NS}/clean_listings}"
export HDFS_CLEAN_reviews="${HDFS_CLEAN_reviews:-${HDFS_NS}/clean_reviews}"
export HDFS_CLEAN_calendar="${HDFS_CLEAN_calendar:-${HDFS_NS}/clean_calendar}"

# Cross-file analytics outputs
export HDFS_REVIEWS_BY_NB="${HDFS_REVIEWS_BY_NB:-${HDFS_NS}/reviews_by_neighbourhood}"
export HDFS_CAL_ROLLUP_PER_LISTING="${HDFS_CAL_ROLLUP_PER_LISTING:-${HDFS_NS}/calendar_rollup_per_listing}"
export HDFS_OCCUPANCY_BY_NB="${HDFS_OCCUPANCY_BY_NB:-${HDFS_NS}/occupancy_by_neighbourhood}"
export HDFS_REVIEWS_PER_LISTING_MONTH="${HDFS_REVIEWS_PER_LISTING_MONTH:-${HDFS_NS}/reviews_per_listing_month}"

# Local project paths
export BIN_DIR="${BIN_DIR:-${PROJ_ROOT}/bin}"
export JOBS_DIR="${JOBS_DIR:-${PROJ_ROOT}/jobs}"
export DATA_DIR="${DATA_DIR:-${PROJ_ROOT}/data}"
export LOGS_DIR="${LOGS_DIR:-${PROJ_ROOT}/logs}"
export TMP_DIR="${TMP_DIR:-${PROJ_ROOT}/tmp}"

# Optional per-machine overrides (do not commit config/env.local)
if [[ -f "${PROJ_ROOT}/config/env.local" ]]; then
  # shellcheck disable=SC1091
  source "${PROJ_ROOT}/config/env.local"
fi

# Hadoop streaming JAR (let the shell expand the wildcard; don't quote)
export HSTREAM_JAR=${HSTREAM_JAR:-${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar}

# Defaults for data fetch (can be overridden per-invocation)
export COUNTRY_DEFAULT="${COUNTRY_DEFAULT:-united-states}"
export STATE_DEFAULT="${STATE_DEFAULT:-ca}"
export COUNTY_DEFAULT="${COUNTY_DEFAULT:-san-francisco}"
export DATE_DEFAULT="${DATE_DEFAULT:-$(date +%Y-%m-01)}"

# Backward-compat aliases
export HDFS_CLEAN="${HDFS_CLEAN:-${HDFS_CLEAN_listings}}"
export HDFS_CLEAN_REVIEWS="${HDFS_CLEAN_REVIEWS:-${HDFS_CLEAN_reviews}}"
export HDFS_CLEAN_CALENDAR="${HDFS_CLEAN_CALENDAR:-${HDFS_CLEAN_calendar}}"
