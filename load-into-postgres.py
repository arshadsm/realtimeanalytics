import json
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'group.id': 'clickstream_group',
    'auto.offset.reset': 'earliest'
}

# Set up Kafka Consumer
consumer = Consumer(conf)
consumer.subscribe(['clickstream_events'])

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="postgres",  # Replace with your DB name
    user="postgres",  # Replace with your username
    password="postgres",  # Replace with your password
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Function to insert data into PostgreSQL
def insert_into_postgres(data):
    cursor.execute("""
        INSERT INTO clickstream_events (user_id, event_type, timestamp, product_id, category)
        VALUES (%s, %s, %s, %s, %s)
    """, (data['user_id'], data['event_type'], data['timestamp'], data['product_id'], data['category']))
    conn.commit()

# Consume and process Kafka messages
try:
    while True:
        msg = consumer.poll(1.0)  # Timeout 1 second
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        
        # Decode message (assuming it is in JSON format)
        message_value = msg.value().decode('utf-8')
        if message_value:  # Check if the message is not empty
            data = json.loads(message_value)
        else:
            print("Received an empty message")
        #data = json.loads(message_value)

        # Insert into PostgreSQL
        insert_into_postgres(data)
        print(f"Inserted data: {data}")
        
except KeyboardInterrupt:
    print("Stream processing stopped")

finally:
    # Close connections
    consumer.close()
    cursor.close()
    conn.close()
