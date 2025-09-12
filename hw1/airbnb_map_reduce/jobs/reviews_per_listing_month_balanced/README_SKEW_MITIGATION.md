Reviews per Listing per Month — Balanced Join

Goal
- Demonstrate skew mitigation using a two-field key (listing_id, yyyy-mm) and a custom partitioner that partitions on the first field only (listing_id). This yields secondary sort of months within each listing while ensuring all records for a listing land on the same reducer.

Pipeline
1) Inputs
   - clean_listings/ (11-col TSV)
   - clean_reviews/  (4-col TSV: listing_id, review_id, date, yyyy-mm)

2) Mapper (jobs/reviews_per_listing_month_balanced/mapper.py)
   - Listings → key: (lid, 0000-00), val: L  nb  rt
   - Reviews  → key: (lid, yyyy-mm), val: R  yyyy-mm  review_id
   - The sentinel month 0000-00 makes listing metadata sort before real months per listing.

3) Partitioner & Sorting
   -D stream.num.map.output.key.fields=2
   -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
   -D mapreduce.partition.keypartitioner.options=-k1,1
   (Default comparator already sorts by the entire key → (lid, yyyy-mm))

4) Reducer (jobs/reviews_per_listing_month_balanced/reducer.py)
   - Buffers (yyyy-mm, review_id) pairs and emits rows:
     listing_id  neighbourhood  room_type  yyyy-mm  review_id

How to Run (from repo root)
```bash
hadoop jar $HSTREAM_JAR \
  -D mapreduce.job.name="airbnb-reviews-per-listing-month-balanced" \
  -D mapreduce.job.reduces=12 \
  -D stream.num.map.output.key.fields=2 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -D mapreduce.partition.keypartitioner.options=-k1,1 \
  -files airbnb_map_reduce/jobs/reviews_per_listing_month_balanced/mapper.py,airbnb_map_reduce/jobs/reviews_per_listing_month_balanced/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input  $(source airbnb_map_reduce/bin/env.sh; echo $HDFS_CLEAN_listings) \
  -input  $(source airbnb_map_reduce/bin/env.sh; echo $HDFS_CLEAN_reviews) \
  -output $(source airbnb_map_reduce/bin/env.sh; echo $HDFS_REVIEWS_PER_LISTING_MONTH_BALANCED)
```

Notes
- This approach balances across listings, not within a single very hot listing. For extreme per-listing skew, combine with pre-aggregation (compress reviews to monthly counts) or salting (lid#bucket) + two-phase aggregation.

