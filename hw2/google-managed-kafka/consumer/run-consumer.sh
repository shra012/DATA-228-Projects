#!/bin/bash

# Confluent Kafka Consumer
# Simple consumer using confluent-kafka library

BOOTSTRAP="bootstrap.simple-kafka-cluster.us-central1.managedkafka.lateral-layout-474104-p8.cloud.goog:9092"
TOPIC="orders"

cd /home/hiruzen/consumer

echo "Starting Confluent Kafka Consumer"
echo "   Topic: $TOPIC"
echo ""

source /home/hiruzen/kafka-producer-env/bin/activate
python3 consumer.py \
    -b "$BOOTSTRAP" \
    -t "$TOPIC"
