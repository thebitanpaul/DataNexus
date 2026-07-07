"""
producer.py — simulates a live storefront.
Sends 100 fake order events to the 'new_orders' topic, one per second.
Run:  python producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from config import CONFIG, TOPIC

producer = Producer(CONFIG)


def delivery_report(err, msg):
    """Called once per message to report success/failure of delivery."""
    if err is not None:
        print(f"  ! delivery failed: {err}")


# small pools so the same customers/products recur, like a real store
customers = [f"cust_{i:03d}" for i in range(1, 21)]
products = [f"prod_{i:03d}" for i in range(1, 11)]

print(f"Producing 100 order events to '{TOPIC}' at 1/sec...\n")
for n in range(1, 101):
    event = {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "amount": round(random.uniform(10, 200), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(
        TOPIC,
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)  # serve delivery callbacks
    print(f"  [{n:3d}/100] amount={event['amount']:.2f}  {event['customer_id']}")
    time.sleep(1)

producer.flush()  # wait for any in-flight messages
print("\nDone. All 100 events sent.")
