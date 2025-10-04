# Confluent Kafka Consumer

Lightweight Kafka consumer built with the confluent-kafka Python library for Google Managed Kafka.

## Quick Start

```bash
cd /home/hiruzen/consumer
./run-consumer.sh
```

## Capabilities

- Streams messages from the `orders` topic
- Displays order data in real time
- Uses the confluent-kafka client library
- Authenticates with Google Cloud OAuth

## Repository Contents

```
consumer/
|-- consumer.py            # Main consumer application
|-- run-consumer.sh        # Runner script
|-- .env                   # Configuration
|-- README.md              # Documentation
`-- QUICK_START.txt        # Quick reference
```

## Processing Modes

The consumer can display raw messages or maintain aggregations:
- `console` - print each order as it arrives
- `aggregation` - compute per-product statistics (orders, quantity, revenue, pricing)

## Test the End-to-End Flow

Terminal 1 - consumer:
```bash
cd /home/hiruzen/consumer
./run-consumer.sh
```

Terminal 2 - producer:
```bash
cd /home/hiruzen/producer
./run-python-producer.sh
```

## When to Use This Consumer

Choose this implementation when you need:
- A simple real-time stream of orders
- Minimal overhead compared with Spark jobs
- Quick inspection of raw payloads

Use the PySpark consumer when you need:
- DataFrame operations or windowed aggregations
- More advanced analytic processing

## Related Assets

- Producer: `/home/hiruzen/producer/`
- PySpark consumer: `/home/hiruzen/pyspark/`
- Main README: `/home/hiruzen/README.md`

---

Technology: confluent-kafka Python library  
Authentication: Google Cloud OAuth2
