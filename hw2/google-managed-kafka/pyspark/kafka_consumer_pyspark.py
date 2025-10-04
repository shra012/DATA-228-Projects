"""
PySpark Kafka Streaming Consumer for Google Cloud Managed Kafka
Uses official Google Cloud Managed Kafka Auth Handler
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, avg, count, max as _max, min as _min, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import argparse

parser = argparse.ArgumentParser(description='PySpark Kafka Consumer for Order Stream')
parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True,
                    help='Kafka bootstrap servers')
parser.add_argument('-t', '--topic', dest='topic', type=str, default='orders',
                    help='Kafka topic to consume from')
parser.add_argument('-m', '--mode', dest='mode', type=str, default='console',
                    choices=['console', 'aggregation', 'window'],
                    help='Processing mode: console (print), aggregation (stats), window (time-based)')
args = parser.parse_args()

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

current_dir = os.getcwd()
auth_jar = os.path.join(current_dir, "google-cloud-kafka-pyspark-auth-1.0.0.jar")

print("Initializing PySpark with Google Cloud Managed Kafka authentication...")
print(f"   Auth JAR: {auth_jar}")

spark = SparkSession.builder \
    .appName("KafkaOrderStreamConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.kafka:kafka-clients:3.7.2") \
    .config("spark.jars", auth_jar) \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Spark session created: {spark.version}")
print(f"Connecting to Kafka: {args.bootstrap}")
print(f"Subscribing to topic: {args.topic}")
print(f"Processing mode: {args.mode}")
print(f"Consumer group: data228.consumer.group\n")

# Create JAAS config using Google Cloud's official handler
jaas_config = (
    'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;'
)

print("Configuring Google Cloud Managed Kafka authentication...")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", args.bootstrap) \
    .option("subscribe", args.topic) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "OAUTHBEARER") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("kafka.sasl.login.callback.handler.class", 
            "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler") \
    .option("kafka.group.id", "data228.consumer.group") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Kafka stream schema:")
df.printSchema()

# Parse JSON data
orders_df = df.select(
    from_json(col("value").cast("string"), order_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select(
    col("data.*"),
    col("kafka_timestamp")
)

# Convert timestamp string to timestamp type
orders_df = orders_df.withColumn(
    "order_timestamp", 
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
)

print("Parsed orders schema:")
orders_df.printSchema()

if args.mode == "console":
    print("\nMODE: Console output (incoming orders)")
    print("=" * 80)
    
    query = orders_df \
        .select("order_id", "product", "quantity", "price", "order_timestamp") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

elif args.mode == "aggregation":
    print("\nMODE: Aggregation (product statistics)")
    print("=" * 80)
    
    product_stats = orders_df.groupBy("product").agg(
        count("*").alias("total_orders"),
        _sum("quantity").alias("total_quantity"),
        avg("quantity").alias("avg_quantity"),
        _sum(col("quantity") * col("price")).alias("total_revenue"),
        avg("price").alias("avg_price"),
        _min("price").alias("min_price"),
        _max("price").alias("max_price")
    )
    
    query = product_stats \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

elif args.mode == "window":
    print("\nMODE: Window aggregation (30-second tumbling windows)")
    print("=" * 80)
    
    windowed_stats = orders_df \
        .withWatermark("order_timestamp", "1 minute") \
        .groupBy(
            window(col("order_timestamp"), "30 seconds"),
            col("product")
        ).agg(
            count("*").alias("orders_count"),
            _sum("quantity").alias("total_quantity"),
            _sum(col("quantity") * col("price")).alias("revenue")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product"),
            col("orders_count"),
            col("total_quantity"),
            col("revenue")
        )
    
    query = windowed_stats \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()

try:
    print("\nStreaming query started. Press Ctrl+C to stop...")
    print("=" * 80 + "\n")
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\nStopping streaming query...")
    query.stop()
    spark.stop()
    print("Spark session stopped cleanly")
