from confluent_kafka import Producer, Consumer
import json
from dotenv import load_dotenv
import requests
import os

# Load environment variables from a .env file
load_dotenv()

class KafkaProducer:
    """
    KafkaProducer is a class that encapsulates the functionality for producing messages to a Kafka topic.
    
    Attributes:
        properties_file (str): Path to the configuration file for Kafka producer.
        producer (Producer): Confluent Kafka Producer instance.
    """
    
    def __init__(self, properties_file: str):
        """
        Initializes the KafkaProducer with the given properties file.
        
        Args:
            properties_file (str): Path to the configuration file for Kafka producer.
        """
        # Load environment variables
        self.__CONFLUENT_API = os.getenv("CONFLUENT_API")
        self.__CLUSTER_ID = os.getenv("CLUSTER_ID")
        self.__API_KEY = os.getenv("API_KEY")

        self.properties_file = properties_file
        self.producer = Producer(self.read_config())

    def read_config(self):
        """
        Reads the configuration file and returns a dictionary of configuration parameters.
        
        Returns:
            dict: Configuration parameters for Kafka producer.
        
        Raises:
            FileNotFoundError: If the properties file does not exist.
        """
        config = {}

        # Check if the properties file exists
        if not os.path.exists(self.properties_file):
            raise FileNotFoundError(f"File {self.properties_file} not found")
        
        # Read the properties file line by line
        with open(self.properties_file) as fh:
            for line in fh:
                line = line.strip()
                # Ignore empty lines and comments
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config
    
    def create_topic(self, topic_name) -> int:
        """
        Creates a Kafka topic using the Confluent API.
        
        Args:
            topic_name (str): Name of the topic to be created.
        
        Returns:
            int: HTTP status code of the API response.
        """
        url = f"{self.__CONFLUENT_API}/kafka/v3/clusters/{self.__CLUSTER_ID}/topics"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.__API_KEY}"
        }
        data = {
            "topic_name": topic_name
        }
        response = requests.post(url, headers=headers, json=data)
        return response.status_code
    
    def produce(self, topic, data, key_name):
        """
        Produces a message to the specified Kafka topic.
        
        Args:
            topic (str): Name of the Kafka topic.
            data (dict): Data to be sent as the message value.
            key_name (str): Key for the message.
        """
        try:
            print(f"Producing to topic: {topic}")
            self.producer.produce(topic, 
                                  key=key_name, 
                                  value=json.dumps(data), 
                                  callback=self.delivery_report)
            self.producer.flush()
        except Exception as e:
            print(f"Error producing message: {e}")

    def delivery_report(self, err, msg):
        """
        Callback function to report the delivery status of a message.
        
        Args:
            err (KafkaError): Error information if message delivery failed.
            msg (Message): Message object containing delivery information.
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")