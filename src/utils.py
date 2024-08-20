from confluent_kafka import Consumer
import json
from dotenv import load_dotenv
import requests
import os

# Load environment variables from a .env file
load_dotenv()

def create_topic(topic_name):
    """
    Creates a Kafka topic using the Confluent API.
    
    Args:
        topic_name (str): Name of the topic to be created.
    
    Returns:
        int: HTTP status code of the API response.
    """
    # Retrieve API key, Confluent API URL, and Cluster ID from environment variables
    API_KEY = os.getenv("API_KEY")
    CONFLUENT_API = os.getenv("CONFLUENT_API")
    CLUSTER_ID = os.getenv("CLUSTER_ID")

    # Construct the URL for the API request
    url = f"{CONFLUENT_API}/kafka/v3/clusters/{CLUSTER_ID}/topics"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {API_KEY}"
    }
    data = {
        "topic_name": topic_name
    }
    
    # Send a POST request to create the topic
    response = requests.post(url, headers=headers, json=data)
    return response.status_code

def consume(topic, config):
    """
    Consumes messages from a specified Kafka topic.
    
    Args:
        topic (str): Name of the Kafka topic to consume messages from.
        config (dict): Configuration parameters for the Kafka consumer.
    
    Returns:
        dict: The consumed message value as a JSON object.
    """
    # Set the consumer group ID and offset reset policy
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    # Create a new consumer instance
    consumer = Consumer(config)

    # Subscribe to the specified topic
    consumer.subscribe([topic])

    # Poll the topic for messages
    msg = consumer.poll(-1)
    
    # Close the consumer
    consumer.close()
    
    # Check if a message was received and if there were no errors
    if msg is not None and msg.error() is None:
        # Decode the message key and value
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
        
        # Return the message value as a JSON object
        return json.loads(value)