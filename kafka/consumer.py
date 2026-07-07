"""
consumer.py — reads order events, keeps only high-value ones (> 50),
prints them, and writes them to a local JSON file that stands in for a
micro-batch Silver landing zone.
Run:  python consumer.py     (Ctrl+C to stop)
"""

import json

from confluent_kafka import Consumer
from config import CONFIG, TOPIC

# A consumer needs a group id and a starting position.
# 'earliest' = read the topic from the beginning, so we catch every event.
consumer_conf = {
    **CONFIG,
    "group.id": "order-filter-1",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

OUT_FILE = "high_value_orders.json"
THRESHOLD = 50
high_value = []

print(f"Consuming '{TOPIC}', keeping amount > {THRESHOLD}. Ctrl+C to stop.\n")
try:
    while True:
        msg = consumer.poll(1.0)  # wait up to 1s for a message
        if msg is None:
            continue
        if msg.error():
            print(f"  ! consumer error: {msg.error()}")
            continue

        order = json.loads(msg.value().decode("utf-8"))
        if order["amount"] > THRESHOLD:
            print(f"  HIGH-VALUE  amount={order['amount']:.2f}  order={order['order_id'][:8]}")
            high_value.append(order)
            # rewrite the growing micro-batch file so it's always current
            with open(OUT_FILE, "w") as f:
                json.dump(high_value, f, indent=2)

except KeyboardInterrupt:
    print(f"\nStopped. {len(high_value)} high-value orders written to {OUT_FILE}")
finally:
    consumer.close()
