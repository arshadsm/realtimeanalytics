import random
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Simulate product categories
categories = ["electronics", "clothing", "home_appliances", "books", "sports"]

# Simulate event types
event_types = ["product_view", "cart_add", "purchase"]

# Generate random user IDs and product IDs
def generate_random_id(prefix, size=5):
    return f"{prefix}{random.randint(10**(size-1), 10**size - 1)}"

# Generate a single event
def generate_event():
    return {
        "user_id": generate_random_id("U"),
        "event_type": random.choice(event_types),
        "timestamp": datetime.utcnow().isoformat(),
        "product_id": generate_random_id("P"),
        "category": random.choice(categories)
    }

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],  
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Send data to Kafka
def generate_clickstream_data():
    while True:
        event = generate_event()
        producer.send("clickstream_events", value=event)  
        print(f"Sent: {event}")
        time.sleep(1)  # Simulate 1 event per second

if __name__ == "__main__":
    try:
        generate_clickstream_data()
    except KeyboardInterrupt:
        print("\nStopping data generation...")
