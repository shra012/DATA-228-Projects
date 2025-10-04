#!/bin/bash

# PySpark Kafka Consumer with custom Google Cloud OAuth Handler
# Uses the compiled JAR for OAuth authentication

BOOTSTRAP="bootstrap.simple-kafka-cluster.us-central1.managedkafka.lateral-layout-474104-p8.cloud.goog:9092"
TOPIC="orders"
MODE="${1:-console}"

cd /home/hiruzen/pyspark

echo "Starting PySpark Kafka Consumer with Google Cloud OAuth"
echo "   Mode: $MODE"
echo "   JAR: google-cloud-kafka-pyspark-auth-1.0.0.jar"
echo ""

python3 kafka_consumer_pyspark.py \
    -b "$BOOTSTRAP" \
    -t "$TOPIC" \
    -m "$MODE"