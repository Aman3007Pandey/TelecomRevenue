# config/kafka_config.py
import os
from dotenv import load_dotenv

# Load Kafka environment variables from kafka.env file
load_dotenv('kafka.env')

kafka_options = {
    "kafkaBootstrapServers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "kafkaGroupId": os.getenv("KAFKA_GROUP_ID"),
    "kafkaAutoOffsetReset": os.getenv("KAFKA_AUTO_OFFSET_RESET"),
    "kafkaTopicName": os.getenv("KAFKA_TOPIC_NAME"),
    "kafkaFileName": os.getenv("KAFKA_FILE_NAME")
}
