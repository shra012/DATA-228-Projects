# PySpark Kafka Consumer

PySpark Structured Streaming consumer for Google Managed Kafka using the DataFrame API.

## Quick Start

```bash
cd /home/hiruzen/pyspark
./run-pyspark-consumer.sh console
```

## Processing Modes

Console mode:
```bash
./run-pyspark-consumer.sh console
```
Displays orders as they arrive.

Aggregation mode:
```bash
./run-pyspark-consumer.sh aggregation
```
Produces per-product statistics for quantity, revenue, and pricing.

Window mode:
```bash
./run-pyspark-consumer.sh window
```
Maintains 30-second tumbling window metrics.

## Repository Contents

```
pyspark/
|-- kafka_consumer_pyspark.py                 # Structured Streaming application
|-- google-cloud-kafka-pyspark-auth-1.0.0.jar # OAuth authentication helper, should be built from source
|-- oauth-handler/                            # Source code for the auth handler
|-- run-pyspark-consumer.sh                   # Runner script
|-- .env                                      # Configuration
`-- README.md                                 # This file
```

## Features

- Uses the Spark DataFrame API for stream processing
- Supports console, aggregation, and windowed analysis modes
- Integrates with Google Cloud OAuth through the bundled JAR
- Provides low-latency visibility into the order stream

## Testing the Pipeline

Terminal 1 - PySpark consumer:
```bash
cd /home/hiruzen/pyspark
./run-pyspark-consumer.sh console
```

Terminal 2 - Producer:
```bash
cd /home/hiruzen/producer
./run-python-producer.sh
```

## Related Resources

- Producer: `/home/hiruzen/producer/`
- Lightweight consumer: `/home/hiruzen/consumer/`
- Main README: `/home/hiruzen/README.md`

---

Technology: PySpark Structured Streaming  
Authentication: Google Cloud OAuth2 (custom JAR)
