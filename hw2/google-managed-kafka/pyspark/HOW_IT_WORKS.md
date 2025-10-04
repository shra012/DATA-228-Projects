# How the PySpark and Google Managed Kafka Integration Works

## Architecture Overview

Components involved in the streaming pipeline:
- PySpark application (`kafka_consumer_pyspark.py`)
- Google Cloud Managed Kafka brokers
- Google Cloud IAM for OAuth-based authentication

Messages flow from Managed Kafka into Spark Structured Streaming where they are parsed, transformed, and emitted to the configured sink.

## Component Breakdown

### PySpark Application
- Builds a `SparkSession` with the Kafka package and the Google auth JAR.
- Reads from Kafka using Structured Streaming and parses JSON payloads into DataFrames.
- Supports multiple processing modes (console output, aggregations, windowed aggregations).

Example configuration:
```python
spark = SparkSession.builder \
    .appName("KafkaOrderStreamConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.kafka:kafka-clients:3.7.2") \
    .config("spark.jars", "google-cloud-kafka-pyspark-auth-1.0.0.jar") \
    .getOrCreate()
```

### Authentication JAR (`google-cloud-kafka-pyspark-auth-1.0.0.jar`)
- Bundles Google's `GcpLoginCallbackHandler` implementation along with its dependencies.
- Uses Maven shading to relocate Google dependencies and avoid conflicts with Spark's classpath.
- Exposes the handler via `com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler` for SASL OAUTHBEARER flows.

### Managed Kafka Cluster
- Requires `SASL_SSL` with the `OAUTHBEARER` SASL mechanism.
- Provides regional bootstrap endpoints (`bootstrap.<cluster>.<region>.managedkafka.<project>.cloud.goog:9092`).
- Validates presented tokens with Google IAM before establishing the connection.

### Google Cloud IAM
- Application Default Credentials (ADC) supply service-account based OAuth tokens.
- Required roles for the service account include `roles/managedkafka.client`, `roles/iam.serviceAccountTokenCreator`, and `roles/iam.serviceAccountOpenIdTokenCreator`.
- Tokens are refreshed automatically by the callback handler when they approach expiration.

## Connection Sequence

1. Spark loads the Google auth JAR.
2. The Kafka client starts the SASL handshake using the OAUTHBEARER mechanism.
3. `GcpLoginCallbackHandler` requests an access token through ADC.
4. IAM issues a short-lived OAuth token.
5. The token is returned to Kafka, which verifies it with IAM.
6. Once validated, streaming begins and Spark processes records.

## Key Kafka Reader Options

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "bootstrap....goog:9092") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "OAUTHBEARER") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;") \
    .option("kafka.sasl.login.callback.handler.class",
            "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler") \
    .option("kafka.group.id", "data228.consumer.group") \
    .option("startingOffsets", "latest") \
    .load()
```

## Troubleshooting Reference

- **`NoSuchMethodError` or class conflicts** - confirm the auth JAR relocates Google dependencies (Maven shading plugin).
- **Authentication failures** - verify the service account roles listed above and ensure the workload is using ADC.
- **Connection timeouts** - confirm the client is on a network with access to the Managed Kafka VPC subnets.
- **No messages received** - use `startingOffsets="earliest"` for historical data or publish new messages through the producer.

## Performance Notes

- Expect 1-2 GB of memory overhead for a small Structured Streaming job.
- Throughput depends on message size, processing complexity, and available cluster resources; tens of thousands of messages per second are common for lightweight transformations.
- The shaded auth JAR is roughly 12 MB and adds minimal startup time.

## Summary

The shaded authentication JAR bridges Spark's Kafka client with Google Cloud's OAuth requirements. With ADC and the proper IAM roles, the handler continuously refreshes tokens so the PySpark job can focus on processing the streaming data rather than managing authentication details.
