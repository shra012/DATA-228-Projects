#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
source "./env.sh"
cd "${PROJ_ROOT}"

mkdir -p "${LOGS_DIR}"

log() { echo "[$(date +'%F %T')] $*"; }

submit_job() {
  local job_key="$1" job_name="$2" mapper_path="$3" reducer_path="$4" input_path="$5" output_path="$6"

  # output must not exist
  hdfs dfs -rm -r -skipTrash "${output_path}" >/dev/null 2>&1 || true

  local ts="$(date +%F_%H%M%S)"
  local main_log="${LOGS_DIR}/${job_key}_${ts}.log"

  log "Running ${job_key} (${job_name})"
  hadoop jar ${HSTREAM_JAR} \
    -D mapreduce.job.name="${job_name}" \
    -D mapreduce.job.reduces=1 \
    -files "${mapper_path},${reducer_path}" \
    -mapper "python3 $(basename "${mapper_path}")" \
    -reducer "python3 $(basename "${reducer_path}")" \
    -input "${input_path}" \
    -output "${output_path}" |&
    tee "${main_log}"

  log "Driver log saved: ${main_log}"
  echo
}

# Supports multiple -input paths (pass inputs as an array-like string: path1::path2::path3)
submit_job_multi_input() {
  local job_key="$1" job_name="$2" mapper_path="$3" reducer_path="$4" input_paths_joined="$5" output_path="$6"

  hdfs dfs -rm -r -skipTrash "${output_path}" >/dev/null 2>&1 || true

  local ts="$(date +%F_%H%M%S)"
  local main_log="${LOGS_DIR}/${job_key}_${ts}.log"

  IFS='::' read -r -a inputs <<<"${input_paths_joined}"
  local input_flags=()
  for p in "${inputs[@]}"; do
    [[ -n "$p" ]] && input_flags+=( -input "$p" )
  done

  log "Running ${job_key} (${job_name}) with ${#input_flags[@]} inputs"
  hadoop jar ${HSTREAM_JAR} \
    -D mapreduce.job.name="${job_name}" \
    -D mapreduce.job.reduces=1 \
    -files "${mapper_path},${reducer_path}" \
    -mapper "python3 $(basename "${mapper_path}")" \
    -reducer "python3 $(basename "${reducer_path}")" \
    "${input_flags[@]}" \
    -output "${output_path}" |&
    tee "${main_log}"

  log "Driver log saved: ${main_log}"
  echo
}

usage() {
  cat <<EOF
Usage: $0 {data_prep|avg_by_nb_rt|budget_supply|budget_supply_ranked|prep_reviews|prep_calendar|join_listings_reviews|agg_nb_counts|cal_rollup_per_listing|join_listings_calendar|agg_occupancy_by_nb} [--input=listings|reviews|calendar|all]

Notes:
  --input (or -i) applies only to 'data_prep'. Default: listings
  Use 'prep_reviews' and 'prep_calendar' to clean the other raw datasets.
    listings  -> ${HDFS_RAW}/listings
    reviews   -> ${HDFS_RAW}/reviews
    calendar  -> ${HDFS_RAW}/calendar
    all       -> ${HDFS_RAW}
EOF
}

JOB="${1:-}"
shift || true
RAW_SUBSET="listings" # default for data_prep

# Parse optional flags
while [[ $# -gt 0 ]]; do
  case "$1" in
  --input=* | -i=*)
    RAW_SUBSET="${1#*=}"
    shift
    ;;
  --input | -i)
    RAW_SUBSET="${2:-}"
    shift 2
    ;;
  -*)
    echo "Unknown option: $1"
    usage
    exit 1
    ;;
  *)
    echo "Unexpected arg: $1"
    usage
    exit 1
    ;;
  esac
done

[[ -z "${JOB}" ]] && {
  usage
  exit 1
}

# Resolve input path for data_prep
resolve_data_prep_input() {
  case "${RAW_SUBSET}" in
  listings) echo "${HDFS_RAW}/listings" ;;
  reviews) echo "${HDFS_RAW}/reviews" ;;
  calendar) echo "${HDFS_RAW}/calendar" ;;
  all) echo "${HDFS_RAW}" ;;
  *)
    echo "Invalid --input value: '${RAW_SUBSET}'. Use listings|reviews|calendar|all." >&2
    exit 1
    ;;
  esac
}

# Ensure raw subdirs exist (harmless if they already do)
hdfs dfs -mkdir -p "${HDFS_RAW}"/{listings,reviews,calendar}

case "${JOB}" in
data_prep)
  INPUT_PATH="$(resolve_data_prep_input)"
  submit_job \
    "data_prep_${RAW_SUBSET}" \
    "airbnb-data-prep (${RAW_SUBSET})" \
    "${JOBS_DIR}/data_prep/mapper.py" \
    "${JOBS_DIR}/data_prep/reducer.py" \
    "${INPUT_PATH}" \
    "${HDFS_CLEAN_listings}"
  ;;
prep_reviews)
  submit_job \
    "prep_reviews" \
    "airbnb-prep-reviews" \
    "${JOBS_DIR}/prep_reviews/mapper.py" \
    "${JOBS_DIR}/prep_reviews/reducer.py" \
    "${HDFS_RAW}/reviews" \
    "${HDFS_CLEAN_reviews}"
  ;;
prep_calendar)
  submit_job \
    "prep_calendar" \
    "airbnb-prep-calendar" \
    "${JOBS_DIR}/prep_calendar/mapper.py" \
    "${JOBS_DIR}/prep_calendar/reducer.py" \
    "${HDFS_RAW}/calendar" \
    "${HDFS_CLEAN_calendar}"
  ;;
avg_by_nb_rt)
  submit_job \
    "avg_by_nb_rt" \
    "airbnb-avg-by-nb-rt" \
    "${JOBS_DIR}/job_avg/mapper.py" \
    "${JOBS_DIR}/job_avg/reducer.py" \
    "${HDFS_CLEAN_listings}" \
    "${HDFS_AVG}"
  ;;
budget_supply)
  submit_job \
    "budget_supply" \
    "airbnb-budget-supply" \
    "${JOBS_DIR}/job_budget/mapper.py" \
    "${JOBS_DIR}/job_budget/reducer.py" \
    "${HDFS_CLEAN_listings}" \
    "${HDFS_BUDGET}"
  ;;
budget_supply_ranked)
  submit_job \
    "budget_supply_ranked" \
    "airbnb-budget-ranking" \
    "${JOBS_DIR}/job_rank/mapper.py" \
    "${JOBS_DIR}/job_rank/reducer.py" \
    "${HDFS_BUDGET}" \
    "${HDFS_BUDGET_RANK}"
  ;;
join_listings_reviews)
  submit_job_multi_input \
    "join_listings_reviews" \
    "airbnb-join-listings-reviews" \
    "${JOBS_DIR}/join_listings_reviews/mapper.py" \
    "${JOBS_DIR}/join_listings_reviews/reducer.py" \
    "${HDFS_CLEAN_listings}::${HDFS_CLEAN_reviews}" \
    "${HDFS_REVIEWS_BY_NB}.raw"
  ;;
agg_nb_counts)
  submit_job \
    "agg_nb_counts" \
    "airbnb-agg-nb-counts" \
    "${JOBS_DIR}/agg_sum/mapper.py" \
    "${JOBS_DIR}/agg_sum/reducer.py" \
    "${HDFS_REVIEWS_BY_NB}.raw" \
    "${HDFS_REVIEWS_BY_NB}"
  ;;
cal_rollup_per_listing)
  submit_job \
    "cal_rollup_per_listing" \
    "airbnb-cal-rollup-per-listing" \
    "${JOBS_DIR}/cal_rollup/mapper.py" \
    "${JOBS_DIR}/cal_rollup/reducer.py" \
    "${HDFS_CLEAN_calendar}" \
    "${HDFS_CAL_ROLLUP_PER_LISTING}"
  ;;
join_listings_calendar)
  submit_job_multi_input \
    "join_listings_calendar" \
    "airbnb-join-listings-calendar" \
    "${JOBS_DIR}/join_listings_calendar/mapper.py" \
    "${JOBS_DIR}/join_listings_calendar/reducer.py" \
    "${HDFS_CLEAN_listings}::${HDFS_CAL_ROLLUP_PER_LISTING}" \
    "${HDFS_OCCUPANCY_BY_NB}.raw"
  ;;
agg_occupancy_by_nb)
  submit_job \
    "agg_occupancy_by_nb" \
    "airbnb-agg-occupancy-by-nb" \
    "${JOBS_DIR}/agg_two_ints/mapper.py" \
    "${JOBS_DIR}/agg_two_ints/reducer.py" \
    "${HDFS_OCCUPANCY_BY_NB}.raw" \
    "${HDFS_OCCUPANCY_BY_NB}"
  ;;
*)
  usage
  exit 1
  ;;
esac
