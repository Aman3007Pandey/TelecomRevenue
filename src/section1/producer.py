""" Fetch data from s3 and push data to kafka topic"""
import logging
import sys
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.kafka_config import kafka_options
from config.aws_config import aws_options
from src.utility.week_sync import get_current_week_number
from src.utility.aws_connection import AWSConnector

# Kafka broker details
bootstrap_servers = kafka_options["kafkaBootstrapServers"]
topic = kafka_options["kafkaTopicName"]
file_key=kafka_options["kafkaFileName"]

aws_connector=AWSConnector()
# Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'linger.ms': 10,  # Add a small delay to batch messages
    'retries': 3,     # Number of retries on failure
    'acks': 'all'     # Wait for all replicas to acknowledge the message
}

# spark-sesssion
spark = SparkSession.builder \
            .appName("new app") \
            .getOrCreate()
# Function to process CSV to JSON and push to Kafka
def process_csv_to_json_and_push(week_number):
    """Processes the rev1.csv file, filters data for a specified week number, 
    converts to JSON, and pushes the data to a Kafka topic.

    :param week_number: The week number to filter data by.
    :return type: None
    """
    try:
        # Read CSV file into DataFrame
        bucket_name = aws_options["awsS3BucketName"]
        result_df=aws_connector.fetch_data_from_s3_new(bucket_name)
        logging.info("Columns in DataFrame: %s", result_df.columns)
        logging.info("Schema of DataFrame:")
        logging.info(result_df.dtypes)

        # Filter data for the specified week number
        result_df = result_df.withColumn("week_number", col("week_number").cast("integer"))
        df_filtered = result_df[result_df["week_number"] == week_number]

        message_count = df_filtered.count()
        logging.info("Total number of messages to be sent: %d", message_count)

        # Start Kafka producer
        producer = Producer(producer_config)

        # Delivery report callback function
        def delivery_report(err, msg):
            if err is not None:
                logging.error('Message delivery failed: %s', err)
            else:
                logging.info('Message delivered to %s [%d]', msg.topic(), msg.partition())

        # Counter for the number of messages sent
        sent_count = 0

        # Push JSON data to Kafka
        for row in df_filtered.toJSON().collect():
            logging.debug("Processing row: %s", row)

    # Convert row to JSON string
            json_data = row

            while True:
                try:
                    producer.produce(topic, key=None, value=json_data, callback=delivery_report)
                    sent_count += 1
                    producer.poll(0)  # Serve delivery reports for previous sends
                    break  # Exit loop if produce is successful
                except BufferError :
                    logging.error("Local producer queue is full \
                                  (%d messarges awaiting delivery): try again", len(producer))
                    producer.poll(1)  # Wait 1 second before retrying

# Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()

        logging.info("Data for week %s pushed to Kafka successfully", week_number)
        logging.info("Total number of messages sent: %d", sent_count)

    except Exception as error:
        logging.error("An error occurred during processing: %s", str(error))

if __name__ == "__main__":
    # Example: Process data for week number 22
    week_number = get_current_week_number() 
    # Confirming connection to Kafka
    try:
        # Create Kafka producer instance
        producer = Producer(producer_config)
        logging.info("Connected to Kafka successfully")
    except Exception as e:
        logging.error("Failed to connect to Kafka: %s", str(e))
        sys.exit(1)

    # Start Kafka producer
    logging.info("Starting Kafka producer...")
    process_csv_to_json_and_push(week_number)
