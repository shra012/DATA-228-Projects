SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# Choose which raw subset to prep: listings | reviews | calendar | all
RAW ?= listings

# Absolute path to this Makefile's directory (portable even when invoked with -f)
SELF := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: help init fetch check data_prep avg budget rank all quick clean_outputs \
        prep_reviews prep_calendar reviews_by_nb occupancy

help:
	@echo "Airbnb MapReduce — common targets:"
	@echo "  make init                # create HDFS base tree"
	@echo "  make fetch               # download InsideAirbnb data + put to HDFS"
	@echo "  make check RAW=...      # verify HDFS raw inputs exist for the chosen RAW"
	@echo "  make data_prep RAW=...  # clean listings (default: listings); use prep_reviews/prep_calendar for others"
	@echo "  make avg|budget|rank     # analytics on cleaned listings"
	@echo "  make reviews_by_nb       # listings ⨝ reviews → counts per neighbourhood"
	@echo "  make occupancy           # listings ⨝ calendar → booked/total per neighbourhood"
	@echo "  make clean_outputs       # remove MR outputs so jobs can re-run"
	@echo "  make all RAW=...         # run pipeline (no init/fetch); RAW=listings|reviews|calendar|all"

# 1) HDFS base tree
init:
	bash $(SELF)bin/hdfs_init.sh

# 2) Download via curl and put to HDFS (/raw/*)
fetch:
	bash $(SELF)bin/fetch_and_put.sh

# 2.5) Preflight checks for required raw inputs in HDFS
check:
	@bash -lc 'set -euo pipefail; source "$(SELF)bin/env.sh"; \
	  case "$(RAW)" in \
	    listings) reqs="$$HDFS_RAW/listings";; \
	    reviews)  reqs="$$HDFS_RAW/listings $$HDFS_RAW/reviews";; \
	    calendar) reqs="$$HDFS_RAW/listings $$HDFS_RAW/calendar";; \
	    all)      reqs="$$HDFS_RAW/listings $$HDFS_RAW/reviews $$HDFS_RAW/calendar";; \
	    *) echo "Invalid RAW=$(RAW). Use listings|reviews|calendar|all."; exit 1;; \
	  esac; \
	  for d in $$reqs; do \
	    echo "[CHECK] HDFS dir: $$d"; \
	    hdfs dfs -test -d "$$d" || { echo "ERROR: Missing HDFS dir: $$d"; exit 1; }; \
	    if ! hdfs dfs -ls -C "$$d" 2>/dev/null | head -n1 | grep -q .; then \
	      echo "ERROR: No files under: $$d"; exit 1; \
	    fi; \
	  done; \
	  echo "Preflight OK for RAW=$(RAW)"'

# 3) Run only data_prep (uses RAW variable)
data_prep:
	bash $(SELF)bin/run.sh data_prep --input=$(RAW)

# 4) Downstream jobs
avg:
	bash $(SELF)bin/run.sh avg_by_nb_rt

budget:
	bash $(SELF)bin/run.sh budget_supply

rank:
	bash $(SELF)bin/run.sh budget_supply_ranked

# Everything, in the correct order like listings does (data_prep → avg → budget → rank)
all:
	@$(MAKE) check RAW=$(RAW)
	@if [ "$(RAW)" = "listings" ]; then \
		$(MAKE) data_prep avg budget rank; \
	elif [ "$(RAW)" = "reviews" ]; then \
		$(MAKE) reviews_by_nb; \
	elif [ "$(RAW)" = "calendar" ]; then \
		$(MAKE) occupancy; \
	elif [ "$(RAW)" = "all" ]; then \
		$(MAKE) data_prep prep_reviews prep_calendar avg budget rank reviews_by_nb occupancy; \
	else \
		echo "Invalid RAW=$(RAW). Use listings|reviews|calendar|all."; exit 1; \
	fi

# Quick dev loop (assumes HDFS already set up and data present)
quick: data_prep avg

# Remove only MR outputs (safe to re-run)
clean_outputs:
	@bash -lc 'source "$(SELF)bin/env.sh"; \
	  hdfs dfs -rm -r -skipTrash \
	    "$$HDFS_CLEAN_listings" \
	    "$$HDFS_AVG" \
	    "$$HDFS_BUDGET" \
	    "$$HDFS_BUDGET_RANK" \
	    "$$HDFS_CLEAN_reviews" \
	    "$$HDFS_CLEAN_calendar" \
	    "$$HDFS_REVIEWS_BY_NB" \
	    "$$HDFS_REVIEWS_BY_NB.raw" \
	    "$$HDFS_CAL_ROLLUP_PER_LISTING" \
	    "$$HDFS_OCCUPANCY_BY_NB" \
	    "$$HDFS_OCCUPANCY_BY_NB.raw" || true'

# New: preprocess reviews and calendar raw datasets
prep_reviews:
	bash $(SELF)bin/run.sh prep_reviews

prep_calendar:
	bash $(SELF)bin/run.sh prep_calendar

# Cross-file: total reviews per neighbourhood
reviews_by_nb: prep_reviews data_prep
	bash $(SELF)bin/run.sh join_listings_reviews
	bash $(SELF)bin/run.sh agg_nb_counts

# Cross-file: occupancy proxy by neighbourhood (booked/total days)
occupancy: prep_calendar data_prep
	bash $(SELF)bin/run.sh cal_rollup_per_listing
	bash $(SELF)bin/run.sh join_listings_calendar
	bash $(SELF)bin/run.sh agg_occupancy_by_nb
