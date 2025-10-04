#!/bin/bash
# Python Producer for Google Managed Kafka
# Usage: 
#   ./run-python-producer.sh                    # Run continuously at default rate (0.2 msg/s)
#   ./run-python-producer.sh 50                 # Send 50 messages at default rate
#   ./run-python-producer.sh 50 1.0             # Send 50 messages at 1 msg/s
#   ./run-python-producer.sh 0 5.0              # Run continuously at 5 msg/s

BOOTSTRAP="bootstrap.simple-kafka-cluster.us-central1.managedkafka.lateral-layout-474104-p8.cloud.goog:9092"
TOPIC="orders"
NUM_MESSAGES=${1:-0}   # Default to 0 (infinite) if not specified
RATE=${2:-0.2}         # Default to 0.2 messages/second (1 every 5 seconds)

if [ "$NUM_MESSAGES" -eq 0 ]; then
    echo "Starting continuous producer (infinite mode)"
    echo "   Rate: $RATE messages/second"
    echo "   Press Ctrl+C to stop"
else
    echo "Starting producer with $NUM_MESSAGES messages"
    echo "   Rate: $RATE messages/second"
fi

source /home/hiruzen/kafka-producer-env/bin/activate
python3 /home/hiruzen/producer/producer.py \
    -b "$BOOTSTRAP" \
    -t "$TOPIC" \
    -n "$NUM_MESSAGES" \
    -r "$RATE"
