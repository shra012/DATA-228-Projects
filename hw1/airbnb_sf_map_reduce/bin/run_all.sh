#!/usr/bin/env bash
set -euo pipefail

# Always resolve to repo root, then load env
cd "$(dirname "$0")"
source "./env.sh"
cd "${PROJ_ROOT}"

mkdir -p "${LOGS_DIR}"

log() { echo "[$(date +'%F %T')] $*"; }

# Helper to submit a Hadoop Streaming job and capture logs
submit_job() {
  local job_key="$1"      # e.g., data_prep, avg_by_nb_rt
  local job_name="$2"     # human-readable job name
  local mapper_path="$3"  # absolute path to mapper.py (in JOBS_DIR)
  local reducer_path="$4" # absolute path to reducer.py
  local input_path="$5"   # HDFS input
  local output_path="$6"  # HDFS output

  # Ensure output path does not exist
  hdfs dfs -rm -r -skipTrash "${output_path}" >/dev/null 2>&1 || true

  local ts
  ts="$(date +%F_%H%M%S)"
  local main_log="${LOGS_DIR}/${job_key}_${ts}.log"
  local yarn_log="${LOGS_DIR}/${job_key}_yarn_${ts}.log"

  log "Submitting ${job_key} (${job_name})"
  # Run job, capture stdout+stderr to a local log (and console)
  hadoop jar ${HSTREAM_JAR} \
    -D mapreduce.job.name="${job_name}" \
    -D mapreduce.job.reduces=1 \
    -files "${mapper_path},${reducer_path}" \
    -mapper "python3 $(basename "${mapper_path}")" \
    -reducer "python3 $(basename "${reducer_path}")" \
    -input "${input_path}" \
    -output "${output_path}" |&
    tee "${main_log}"

  # Try to extract the YARN applicationId and Tracking URL from the driver log
  local app_id=""
  local track_url=""
  app_id="$(grep -oE 'application_[0-9_]+' "${main_log}" | tail -1 || true)"
  track_url="$(awk -F'Tracking URL: ' '/Tracking URL:/ {print $2}' "${main_log}" | tail -1 || true)"

  if [[ -n "${app_id}" ]]; then
    log "YARN applicationId: ${app_id}"
    [[ -n "${track_url}" ]] && log "Tracking URL: ${track_url}"
    # Fetch YARN aggregated logs (may require log aggregation to be enabled)
    if yarn logs -applicationId "${app_id}" >"${yarn_log}" 2>/dev/null; then
      log "YARN logs saved: ${yarn_log}"
    else
      log "WARN: Could not retrieve YARN logs (aggregation disabled or insufficient perms)."
    fi
  else
    log "WARN: Could not detect YARN applicationId in ${main_log}"
  fi

  echo
}

# Ensure input dirs exist; do NOT pre-create MR output dirs
hdfs dfs -mkdir -p "${HDFS_RAW}"/{listings,reviews,calendar}

# Optional: quick demo upload if a local CSV is present
if [[ -f "${PROJ_ROOT}/san_francisco_listings.csv" ]]; then
  log "Uploading local san_francisco_listings.csv to ${HDFS_RAW}/listings/"
  hdfs dfs -put -f "${PROJ_ROOT}/san_francisco_listings.csv" "${HDFS_RAW}/listings/"
fi

############################
# Job 0: data_prep  raw -> clean
############################
submit_job \
  "data_prep" \
  "airbnb-data-prep" \
  "${JOBS_DIR}/data_prep/mapper.py" \
  "${JOBS_DIR}/data_prep/reducer.py" \
  "${HDFS_RAW}" \
  "${HDFS_CLEAN}"

############################
# Job 1: avg_by_nb_rt  clean -> avg
############################
submit_job \
  "avg_by_nb_rt" \
  "airbnb-avg-by-nb-rt" \
  "${JOBS_DIR}/job_avg/mapper.py" \
  "${JOBS_DIR}/job_avg/reducer.py" \
  "${HDFS_CLEAN}" \
  "${HDFS_AVG}"

############################
# Job 2: budget_supply  clean -> budget
############################
submit_job \
  "budget_supply" \
  "airbnb-budget-supply" \
  "${JOBS_DIR}/job_budget/mapper.py" \
  "${JOBS_DIR}/job_budget/reducer.py" \
  "${HDFS_CLEAN}" \
  "${HDFS_BUDGET}"

############################
# Job 3: budget_supply_ranked  budget -> rank
############################
submit_job \
  "budget_supply_ranked" \
  "airbnb-budget-ranking" \
  "${JOBS_DIR}/job_rank/mapper.py" \
  "${JOBS_DIR}/job_rank/reducer.py" \
  "${HDFS_BUDGET}" \
  "${HDFS_BUDGET_RANK}"

# Quick previews
echo "[CLEAN]"
(hdfs dfs -cat "${HDFS_CLEAN}/part-*" 2>/dev/null | head -n 8) || true
echo "[AVG]"
(hdfs dfs -cat "${HDFS_AVG}/part-*" 2>/dev/null | head -n 8) || true
echo "[BUDGET]"
(hdfs dfs -cat "${HDFS_BUDGET}/part-*" 2>/dev/null | head -n 8) || true
echo "[RANK]"
(hdfs dfs -cat "${HDFS_BUDGET_RANK}/part-*" 2>/dev/null | head -n 8) || true

log "All jobs complete. Driver logs live in: ${LOGS_DIR}"
