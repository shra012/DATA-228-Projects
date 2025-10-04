# Kafka Python Producer

Continuous streaming producer for Google Managed Kafka that emits synthetic order data.

## Quick Start

Run continuously (default):
```bash
cd /home/hiruzen/producer
./run-python-producer.sh
```

Run with a message limit:
```bash
./run-python-producer.sh 50    # Send 50 messages then stop
```

Stop the producer with `Ctrl+C`.

## Features

- Continuous streaming with optional message cap
- Configurable rate control (`--rate`) expressed as messages per second
- Five-second interval by default (0.2 msg/sec)
- Local logging of each message and Kafka acknowledgements
- Ten synthetic products with randomized quantities and prices

## Project Layout

```
producer/
|-- producer.py                 # Main producer application
|-- tokenprovider.py            # Google OAuth2 authentication helper
|-- .env                        # Environment configuration
|-- requirements.txt            # Python dependencies
|-- run-python-producer.sh      # Convenience runner script
|-- QUICK_START.txt             # Quick reference guide
`-- README.md                   # This file
```

## Usage Options

Scripted execution (recommended):
```bash
./run-python-producer.sh                # Continuous mode
./run-python-producer.sh 100            # Send 100 messages
```

Direct Python execution:
```bash
source /home/hiruzen/kafka-producer-env/bin/activate
python3 producer.py \
    -b "bootstrap.simple-kafka-cluster.us-central1.managedkafka.lateral-layout-474104-p8.cloud.goog:9092" \
    -t "orders" \
    -n 0 \
    -r 0.2
```

## Console Output Example

```
Starting Python producer for topic 'orders'
   Bootstrap: bootstrap.simple-kafka-cluster...
   Messages to send: CONTINUOUS (infinite)
   Rate: 0.2 messages/second (interval: 5.000s)
   Press Ctrl+C to stop

================================================================================
[1] Posted: order-45678 | laptop | Qty: 2 | $1299.99
   Confirmed delivery to partition 0 at offset 123

[2] Posted: order-23456 | mouse | Qty: 1 | $29.99
   Confirmed delivery to partition 1 at offset 456
...
```

## Sample Message Schema

```json
{
  "order_id": "order-12345",
  "product": "laptop",
  "quantity": 2,
  "price": 1299.99,
  "timestamp": "2025-10-04T07:30:00.123456"
}
```

Products sampled: laptop, phone, tablet, monitor, keyboard, mouse, headphones, webcam, speaker, printer.

## Dependencies

Installed in the shared virtual environment (`/home/hiruzen/kafka-producer-env`):
- confluent-kafka==2.11.1
- faker==37.8.0
- google-auth==2.37.0
- python-dotenv==1.1.1

## Authentication

The producer uses Google Cloud Application Default Credentials (ADC) with a custom OAuth2 token provider that creates tokens for Google Managed Kafka.

Key components:
- `tokenprovider.py` - Generates OAuth tokens using Google credentials
- Service account with the roles:
  - `roles/managedkafka.client`
  - `roles/iam.serviceAccountTokenCreator`
  - `roles/iam.serviceAccountOpenIdTokenCreator`

## Configuration

Set values in `.env`:
```bash
KAFKA_BOOTSTRAP_SERVERS=your-bootstrap-server:9092
KAFKA_TOPIC=orders
KAFKA_CLIENT_ID=orders-producer
```

## Coordinated Testing with the Consumer

Terminal 1 - producer:
```bash
cd /home/hiruzen/producer
./run-python-producer.sh
```

Terminal 2 - consumer:
```bash
cd /home/hiruzen/consumer
./run-consumer.sh console
```

Monitor both terminals to validate the end-to-end flow.

## Command-Line Arguments

```bash
python3 producer.py -h
```
```
-b, --bootstrap-servers  Kafka bootstrap server address (required)
-t, --topic-name         Kafka topic name (default: orders)
-n, --num_messages       Number of messages (0 = infinite, default: 0)
-r, --rate               Messages per second (default: 0.2)
```

## Troubleshooting

Authentication errors:
```bash
gcloud auth application-default login
```

Missing dependencies:
```bash
source /home/hiruzen/kafka-producer-env/bin/activate
pip install -r requirements.txt
```

Connection issues:
- Verify the bootstrap server address in `.env`
- Check network connectivity to Google Cloud
- Confirm that the Kafka cluster is running

## Related Documentation

- Main project: `/home/hiruzen/README.md`
- PySpark consumer: `/home/hiruzen/consumer/README.md`
- Project structure: `/home/hiruzen/PROJECT_STRUCTURE.md`
