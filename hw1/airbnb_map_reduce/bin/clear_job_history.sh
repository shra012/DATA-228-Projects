#!/usr/bin/env bash
set -euo pipefail

CONF_DIR="${HADOOP_CONF_DIR:-/home/cloud_user/hadoop/etc/hadoop}"
MR_DONE_DIR="$(grep -hoP '(?<=<name>mapreduce.jobhistory.done-dir</name>\s*<value>)[^<]+' "$CONF_DIR"/*.xml 2>/dev/null | head -n1 || true)"
MR_DONE_DIR="${MR_DONE_DIR:-/mr-history/done}"
TODAY_YMD="$(date +%Y/%m/%d)"

echo "JobHistory dir: $MR_DONE_DIR/$TODAY_YMD"
hdfs dfs -test -e "$MR_DONE_DIR/$TODAY_YMD" &&
  hdfs dfs -rm -r -skipTrash "$MR_DONE_DIR/$TODAY_YMD" ||
  echo "No JobHistory found for today."
