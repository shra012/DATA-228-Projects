SHELL := /usr/bin/env bash

# Choose which raw subset to prep: listings | reviews | calendar | all
RAW ?= listings

.PHONY: init fetch data_prep avg budget rank all quick clean_outputs

# 1) HDFS base tree
init:
	bash bin/hdfs_init.sh

# 2) Download via curl and put to HDFS (/raw/*)
fetch:
	bash bin/fetch_and_put.sh "" "" "" "$(DATE)"

# 3) Run only data_prep (uses RAW variable)
data_prep:
	bash bin/run.sh data_prep --input=$(RAW)

# 4) Downstream jobs
avg:
	bash bin/run.sh avg_by_nb_rt

budget:
	bash bin/run.sh budget_supply

rank:
	bash bin/run.sh budget_supply_ranked

# Everything, in the correct order (data_prep → avg → budget → rank)
all: init fetch data_prep avg budget rank

# Quick dev loop (assumes HDFS already set up and data present)
quick: data_prep avg

# Remove only MR outputs (safe to re-run)
clean_outputs:
	hdfs dfs -rm -r -skipTrash \
	  /projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce/clean \
	  /projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce/avg_by_nb_rt \
	  /projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce/budget_supply \
	  /projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce/budget_supply_ranked || true
