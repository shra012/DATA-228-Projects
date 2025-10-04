import confluent_kafka
import argparse
import json
import random
import time
from datetime import datetime
from tokenprovider import TokenProvider

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True)
parser.add_argument('-t', '--topic-name', dest='topic_name', type=str, default='orders', required=False)
parser.add_argument('-n', '--num_messages', dest='num_messages', type=int, default=0, required=False, 
                    help='Number of messages to send (0 = infinite)')
parser.add_argument('-r', '--rate', dest='rate', type=float, default=0.2, required=False,
                    help='Messages per second (default: 0.2, i.e., 1 message every 5 seconds)')
args = parser.parse_args()

token_provider = TokenProvider()

config = {
    'bootstrap.servers': args.bootstrap,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,
}

producer = confluent_kafka.Producer(config)

PRODUCTS = ['laptop', 'phone', 'tablet', 'monitor', 'keyboard', 'mouse', 'headphones', 'webcam', 'speaker', 'printer']

def callback(error, message):
    if error is not None:
        print(f"Error delivering message: {error}")
        return
    print(
        "   Confirmed delivery to partition "
        f"{message.partition()} at offset {message.offset()}"
    )

if args.rate <= 0:
    print("Rate must be greater than 0")
    exit(1)

sleep_interval = 1.0 / args.rate

print(f"Starting Python producer for topic '{args.topic_name}'")
print(f"   Bootstrap: {args.bootstrap}")
if args.num_messages == 0:
    print(f"   Messages to send: CONTINUOUS (infinite)")
else:
    print(f"   Messages to send: {args.num_messages}")
print(f"   Rate: {args.rate} messages/second (interval: {sleep_interval:.3f}s)")
print(f"   Press Ctrl+C to stop\n")
print("=" * 80)

message_count = 0

try:
    while True:
        message_count += 1
        order_data = {
            'order_id': f'order-{random.randint(10000, 99999)}',
            'product': random.choice(PRODUCTS),
            'quantity': random.randint(1, 10),
            'price': round(random.uniform(10.99, 2999.99), 2),
            'timestamp': datetime.utcnow().isoformat()
        }
        message = json.dumps(order_data).encode('utf-8')
        producer.produce(args.topic_name, message, callback=callback)
        if args.num_messages == 0:
            print(f"[{message_count}] Posted: {order_data['order_id']} | {order_data['product']} | Qty: {order_data['quantity']} | ${order_data['price']}")
        else:
            print(f"[{message_count}/{args.num_messages}] Posted: {order_data['order_id']} | {order_data['product']} | Qty: {order_data['quantity']} | ${order_data['price']}")
        producer.flush()
        if args.num_messages > 0 and message_count >= args.num_messages:
            break
        time.sleep(sleep_interval)
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    print("\nFlushing remaining messages...")
    producer.flush()
    print(f"Producer stopped cleanly. Total messages sent: {message_count}")
    print("=" * 80)
