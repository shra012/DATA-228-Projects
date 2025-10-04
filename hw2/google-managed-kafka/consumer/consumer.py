"""
Python Kafka Streaming Consumer for Google Cloud Managed Kafka
Uses confluent-kafka library with TokenProvider (same as the producer)
"""

import sys
sys.path.insert(0, '/home/hiruzen')

from confluent_kafka import Consumer, KafkaError
from tokenprovider import TokenProvider
import json
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description='Python Kafka Consumer for Order Stream')
parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True,
                    help='Kafka bootstrap servers')
parser.add_argument('-t', '--topic', dest='topic', type=str, default='orders',
                    help='Kafka topic to consume from')
parser.add_argument('-m', '--mode', dest='mode', type=str, default='console',
                    choices=['console', 'aggregation'],
                    help='Processing mode: console (print), aggregation (stats)')
parser.add_argument('--from-beginning', dest='from_beginning', action='store_true',
                    help='Start from beginning of topic')
args = parser.parse_args()

token_provider = TokenProvider()

config = {
    'bootstrap.servers': args.bootstrap,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,
    'group.id': 'data228.consumer.group',
    'auto.offset.reset': 'earliest' if args.from_beginning else 'latest',
    'enable.auto.commit': True,
}

consumer = Consumer(config)

consumer.subscribe([args.topic])

print("Starting Python Kafka Consumer")
print(f"   Bootstrap: {args.bootstrap}")
print(f"   Topic: {args.topic}")
print(f"   Consumer Group: data228.consumer.group")
print(f"   Mode: {args.mode}")
print(f"   Offset: {'earliest' if args.from_beginning else 'latest'}")
print(f"Subscribed to topic '{args.topic}'")
print("Waiting for messages... (Press Ctrl+C to stop)\n")
print("=" * 80)

# Statistics for aggregation mode
product_stats = {}
message_count = 0

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break
        
        # Parse message
        try:
            order_data = json.loads(msg.value().decode('utf-8'))
            message_count += 1
            
            if args.mode == "console":
                print(f"\nOrder #{message_count}:")
                print(f"   Order ID: {order_data.get('order_id', 'N/A')}")
                print(f"   Product:  {order_data.get('product', 'N/A')}")
                print(f"   Quantity: {order_data.get('quantity', 0)}")
                print(f"   Price:    ${order_data.get('price', 0.0):.2f}")
                print(f"   Time:     {order_data.get('timestamp', 'N/A')}")
                print(f"   Partition: {msg.partition()}, Offset: {msg.offset()}")
                print("-" * 80)
                
            elif args.mode == "aggregation":
                product = order_data.get('product', 'unknown')
                quantity = order_data.get('quantity', 0)
                price = order_data.get('price', 0.0)
                revenue = quantity * price
                
                if product not in product_stats:
                    product_stats[product] = {
                        'count': 0,
                        'total_quantity': 0,
                        'total_revenue': 0.0,
                        'prices': []
                    }
                
                product_stats[product]['count'] += 1
                product_stats[product]['total_quantity'] += quantity
                product_stats[product]['total_revenue'] += revenue
                product_stats[product]['prices'].append(price)
                
                if message_count % 10 == 0:
                    print(f"\nProduct statistics (after {message_count} messages):")
                    print(f"{'Product':<15} {'Orders':<10} {'Quantity':<10} {'Revenue':<12} {'Avg Price':<10}")
                    print("=" * 80)
                    for prod, stats in sorted(product_stats.items()):
                        avg_price = sum(stats['prices']) / len(stats['prices']) if stats['prices'] else 0
                        print(f"{prod:<15} {stats['count']:<10} {stats['total_quantity']:<10} "
                              f"${stats['total_revenue']:<11.2f} ${avg_price:<9.2f}")
                    print("=" * 80)
                    
        except json.JSONDecodeError as e:
            print(f"Failed to parse message: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("\n\nStopping consumer...")
    
finally:
    if args.mode == "aggregation" and product_stats:
        print(f"\nFinal product statistics ({message_count} total messages):")
        print(f"{'Product':<15} {'Orders':<10} {'Quantity':<10} {'Revenue':<12} {'Avg Price':<10}")
        print("=" * 80)
        for prod, stats in sorted(product_stats.items()):
            avg_price = sum(stats['prices']) / len(stats['prices']) if stats['prices'] else 0
            print(f"{prod:<15} {stats['count']:<10} {stats['total_quantity']:<10} "
                  f"${stats['total_revenue']:<11.2f} ${avg_price:<9.2f}")
        print("=" * 80)
    
    consumer.close()
    print(f"Consumer closed cleanly. Total messages consumed: {message_count}")
