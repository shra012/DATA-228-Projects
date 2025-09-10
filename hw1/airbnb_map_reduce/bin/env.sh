# bin/env.sh
set -euo pipefail

# HDFS namespace
export HDFS_NS="/projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce"
export HDFS_RAW="${HDFS_NS}/raw"
export HDFS_CLEAN="${HDFS_NS}/clean"
export HDFS_AVG="${HDFS_NS}/avg_by_nb_rt"
export HDFS_BUDGET="${HDFS_NS}/budget_supply"
export HDFS_BUDGET_RANK="${HDFS_NS}/budget_supply_ranked"

# Local project root (adjust if you move it)
export PROJ_ROOT="/home/cloud_user/DATA-228-Projects/hw1/airbnb_sf_map_reduce"
export BIN_DIR="${PROJ_ROOT}/bin"
export JOBS_DIR="${PROJ_ROOT}/jobs"
export DATA_DIR="${PROJ_ROOT}/data"
export LOGS_DIR="${PROJ_ROOT}/logs"
export TMP_DIR="${PROJ_ROOT}/tmp"

# Hadoop streaming JAR (let the shell expand the wildcard; don’t quote)
export HSTREAM_JAR=${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar

# Defaults for data fetch (can be overridden per-invocation)
export COUNTRY_DEFAULT="united-states"
export STATE_DEFAULT="ca"
export COUNTY_DEFAULT="san-francisco"
export DATE_DEFAULT="$(date +%Y-%m-01)"
