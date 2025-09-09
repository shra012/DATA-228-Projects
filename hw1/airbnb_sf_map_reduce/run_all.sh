#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

ROOT=/projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce
RAW=$ROOT/raw
CLEAN=$ROOT/clean
AVG=$ROOT/avg_by_nb_rt
BUDGET=$ROOT/budget_supply
RANK=$ROOT/budget_supply_ranked

# HDFS dirs
hdfs dfs -mkdir -p "$RAW" "$CLEAN" "$AVG" "$BUDGET" "$RANK"

# Upload raw CSV (overwrite)
if [ -f san_francisco_listings.csv ]; then
  hdfs dfs -put -f san_francisco_listings.csv "$RAW/"
fi

# 2.a Data prep
hdfs dfs -rm -r -skipTrash "$CLEAN" >/dev/null 2>&1 || true
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="airbnb-data-prep" \
  -D mapreduce.job.reduces=1 \
  -input "$RAW" \
  -output "$CLEAN" \
  -mapper data_prep/mapper.py \
  -reducer data_prep/reducer.py \
  -file data_prep/mapper.py \
  -file data_prep/reducer.py

# Job 1: avg by (nb, room_type)
hdfs dfs -rm -r -skipTrash "$AVG" >/dev/null 2>&1 || true
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="airbnb-avg-by-nb-rt" \
  -D mapreduce.job.reduces=1 \
  -input "$CLEAN" \
  -output "$AVG" \
  -mapper job_avg/mapper.py \
  -reducer job_avg/reducer.py \
  -file job_avg/mapper.py \
  -file job_avg/reducer.py

# Job 2: budget supply
hdfs dfs -rm -r -skipTrash "$BUDGET" >/dev/null 2>&1 || true
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="airbnb-budget-supply" \
  -D mapreduce.job.reduces=1 \
  -input "$CLEAN" \
  -output "$BUDGET" \
  -mapper job_budget/mapper.py \
  -reducer job_budget/reducer.py \
  -file job_budget/mapper.py \
  -file job_budget/reducer.py

# Ranking (optional)
hdfs dfs -rm -r -skipTrash "$RANK" >/dev/null 2>&1 || true
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="airbnb-budget-ranking" \
  -D mapreduce.job.reduces=1 \
  -input "$BUDGET" \
  -output "$RANK" \
  -mapper job_rank/mapper.py \
  -reducer job_rank/reducer.py \
  -file job_rank/mapper.py \
  -file job_rank/reducer.py

# Previews
echo "[CLEAN]"
hdfs dfs -cat "$CLEAN/part-*" 2>/dev/null | head -n 8
echo "[AVG]"
hdfs dfs -cat "$AVG/part-*" 2>/dev/null | head -n 8
echo "[BUDGET]"
hdfs dfs -cat "$BUDGET/part-*" 2>/dev/null | head -n 8
echo "[RANK]"
hdfs dfs -cat "$RANK/part-*" 2>/dev/null | head -n 8
