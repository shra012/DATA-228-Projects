#!/bin/bash
# Command used to create the Google Managed Kafka cluster

gcloud managed-kafka clusters create simple-kafka-cluster \
    --location=us-central1 \
    --cpu=3 \
    --memory=3GiB \
    --subnets=projects/lateral-layout-474104-p8/regions/us-central1/subnetworks/default

# Additional topic creation (if needed):
# gcloud managed-kafka topics create orders \
#     --cluster=simple-kafka-cluster \
#     --location=us-central1 \
#     --partitions=3 \
#     --replication-factor=2

# Cluster details:
# - Name: simple-kafka-cluster
# - Region: us-central1
# - vCPUs: 3 (minimum allowed by GCP)
# - Memory: 3GiB
# - Network: default VPC
# - Bootstrap: bootstrap.simple-kafka-cluster.us-central1.managedkafka.lateral-layout-474104-p8.cloud.goog:9092