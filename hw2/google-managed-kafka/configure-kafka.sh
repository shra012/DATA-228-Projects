#!/bin/bash

# Script to configure Kafka cluster with specified requirements
# - 1 day retention
# - 2 replicas

CLUSTER_NAME="simple-kafka-cluster"
LOCATION="us-central1"
PROJECT_ID="lateral-layout-474104-p8"

echo "Checking cluster status..."
gcloud managed-kafka clusters describe $CLUSTER_NAME --location=$LOCATION

echo -e "\nWaiting for cluster to be ready..."
while true; do
    STATE=$(gcloud managed-kafka clusters describe $CLUSTER_NAME --location=$LOCATION --format="value(state)" 2>/dev/null)
    echo "Current state: $STATE"
    
    if [ "$STATE" = "ACTIVE" ]; then
        echo "Cluster is ready!"
        break
    elif [ "$STATE" = "ERROR" ] || [ "$STATE" = "FAILED" ]; then
        echo "Cluster creation failed!"
        exit 1
    else
        echo "Waiting for cluster to be ready... (current state: $STATE)"
        sleep 30
    fi
done

echo -e "\nCreating topic with 1-day retention and 2 replicas..."
gcloud managed-kafka topics create orders \
    --cluster=$CLUSTER_NAME \
    --location=$LOCATION \
    --partitions=3 \
    --replication-factor=2 \
    --configs="retention.ms=86400000"  # 24 hours = 86400000 ms

echo -e "\nCluster configuration complete!"
echo "Bootstrap server: $(gcloud managed-kafka clusters describe $CLUSTER_NAME --location=$LOCATION --format="value(gcpConfig.bootstrapServers)")"
echo "Topic created with:"
echo "- Retention: 1 day (86400000 ms)"
echo "- Replicas: 2"
echo "- Partitions: 3"