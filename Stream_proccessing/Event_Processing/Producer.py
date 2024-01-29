from confluent_kafka import Producer
import requests
import json
import logging

# Kafka broker configuration
kafka_config = {
    "bootstrap.servers": "localhost:9092",
}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Kafka producer
producer = Producer(kafka_config)

# Global variable to keep track of message count
message_count = 0

def produce_data(endpoint, topic, start_id, end_id):
    global message_count
    for _id in range(start_id, end_id + 1):
        base_url = f"http://127.0.0.1:5000{endpoint}"
        params = {"id": _id}

        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = json.dumps(response.json())
            producer.produce(topic, key="key", value=data, callback=lambda err, msg: on_delivery(err, msg, data))
            # Flush messages to ensure they are sent
            producer.flush()
        else:
            logger.error(f"Failed to fetch data from {endpoint}. HTTP status code: {response.status_code}")

def on_delivery(err, msg, data):
    global message_count
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        message_count += 1
        logger.info(f"Produced message #{message_count}: {data}")

if __name__ == "__main__":
    # Produce data for the /api/artist endpoint (assuming artist IDs range from 1 to 9787)
    produce_data("/api/artist", "artist_topic", 1, 9787)

    # Produce data for the /api/users endpoint (assuming user IDs range from 1 to 100000)
    produce_data("/api/users", "users_topic", 1, 100000)