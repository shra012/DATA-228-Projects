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

usage() {
  cat <<EOF
Usage: $0 {data_prep|avg_by_nb_rt|budget_supply|budget_supply_ranked} [--input=listings|reviews|calendar|all]

Notes:
  --input (or -i) applies only to 'data_prep'. Default: listings
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
    "${HDFS_CLEAN}"
  ;;
avg_by_nb_rt)
  submit_job \
    "avg_by_nb_rt" \
    "airbnb-avg-by-nb-rt" \
    "${JOBS_DIR}/job_avg/mapper.py" \
    "${JOBS_DIR}/job_avg/reducer.py" \
    "${HDFS_CLEAN}" \
    "${HDFS_AVG}"
  ;;
budget_supply)
  submit_job \
    "budget_supply" \
    "airbnb-budget-supply" \
    "${JOBS_DIR}/job_budget/mapper.py" \
    "${JOBS_DIR}/job_budget/reducer.py" \
    "${HDFS_CLEAN}" \
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
*)
  usage
  exit 1
  ;;
esac
