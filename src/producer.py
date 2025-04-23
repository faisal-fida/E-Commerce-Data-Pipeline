import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG

# Sample data for generating mock orders
CUSTOMERS = [
    {"id": "C001", "name": "Ali Raza", "region": "Punjab"},
    {"id": "C002", "name": "Sara Khan", "region": "Sindh"},
    {"id": "C003", "name": "Ahmed Ali", "region": "KPK"},
    {"id": "C004", "name": "Fatima Zahra", "region": "Balochistan"},
    {"id": "C005", "name": "Usman Khan", "region": "Punjab"},
]

PRODUCTS = [
    {"id": "P001", "name": "Wireless Mouse", "category": "Accessories", "price": 2500},
    {"id": "P002", "name": "Mechanical Keyboard", "category": "Accessories", "price": 5000},
    {"id": "P003", "name": "Gaming Headset", "category": "Audio", "price": 3500},
    {"id": "P004", "name": "USB-C Cable", "category": "Accessories", "price": 500},
    {"id": "P005", "name": "Power Bank", "category": "Electronics", "price": 4000},
]


def generate_order():
    """Generate a random order."""
    customer = random.choice(CUSTOMERS)
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    return {
        "order_id": f"O{random.randint(1000, 9999)}",
        "order_timestamp": datetime.utcnow().isoformat() + "Z",
        "customer_id": customer["id"],
        "customer_name": customer["name"],
        "customer_region": customer["region"],
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": quantity,
    }


def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"]})

    for i in range(100):
        order = generate_order()
        order_json = json.dumps(order)
        producer.produce("orders", order_json.encode("utf-8"), callback=delivery_report)
        producer.poll(0)  # Flush messages
        time.sleep(1)

    producer.flush()  # Wait for any outstanding messages to be delivered


if __name__ == "__main__":
    main()
