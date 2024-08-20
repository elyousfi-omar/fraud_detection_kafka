from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()

class KafkaConsumer:
    """
    A class to consume messages from a Kafka topic using PySpark and write them to a Delta table.

    Attributes:
        kafkaServer (str): Kafka server URL.
        app_name (str): Name of the Spark application.
        topicName (str): Name of the Kafka topic to consume from.
        checkpointLocation (str): Location to store checkpoint data.
        tableLocation (str): Location to store the Delta table.
        spark (SparkSession): Spark session object.
    """
    kafkaServer = os.getenv("CONFLUENT_API")

    def __init__(self, 
                 appName: str, 
                 topicName: str, 
                 tableLocation: str = "/transactions/table", 
                 checkpointLocation: str="/transactions/checkpoint/dir") -> None:
        """
        Initializes the KafkaConsumer with the given parameters.

        Args:
            appName (str): Name of the Spark application.
            topicName (str): Name of the Kafka topic to consume from.
            tableLocation (str, optional): Location to store the Delta table. Defaults to "/transactions/table".
            checkpointLocation (str, optional): Location to store checkpoint data. Defaults to "/transactions/checkpoint/dir".
        """
        # Retrieve the Confluent API URL from environment variables
        self.__CONFLUENT_API = os.getenv("CONFLUENT_API")

        self.app_name = appName
        self.topicName = topicName
        self.checkpointLocation = checkpointLocation
        self.tableLocation = tableLocation

        # Create a Spark session
        self.spark = SparkSession.builder \
                        .appName(appName) \
                        .getOrCreate()

    def consume(self) -> int:
        """
        Consumes messages from the Kafka topic and writes them to a Delta table.

        Returns:
            int: HTTP status code indicating the result of the operation.
        """
        # Read data from the Kafka topic
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.__CONFLUENT_API) \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("subscribe", self.topicName) \
            .load()
        
        # Select the key and value columns from the Kafka data
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # Define a query to write the Kafka data to a Delta table
        query = df.writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", self.checkpointLocation) \
            .start(self.tableLocation)

        # Await termination of the query
        query.awaitTermination()

        return 200