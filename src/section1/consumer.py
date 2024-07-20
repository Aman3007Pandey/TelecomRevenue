""" consumer data from kafa topic, joins using spark and stream it to snowflake"""
import os
import logging
import threading
import time
import sys
from pyspark import SparkConf
from pyspark.sql.functions import from_json,col,to_timestamp,round
from pyspark.sql.types import StringType,StructType,StructField,TimestampType,IntegerType
from pyspark.sql.utils import AnalysisException,StreamingQueryException
from pyspark.sql import SparkSession
from config.mongo_config import mongo_options
from config.snowflake_config import sf_options
from config.kafka_config import kafka_options
from src.utility.snowflake_connection import SnowflakeConnector



# Create a logs folder if it doesn't exist
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

# Configure logging to write to a file in the logs folder
log_file = os.path.join(LOGS_FOLDER, 'mongodb_kafka_join.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Increase spark driver memory to process large data frame
conf = SparkConf()
conf.set("spark.driver.memory", "4g")

class IdleMonitor(threading.Thread):
    """
    A class to monitor the idle time of a Spark Streaming query.
    If the query does not receive new data for a specified timeout, it will stop the query.
    """
    def __init__(self, query, timeout):
        """
        Initialize the IdleMonitor with a Spark Streaming query and a timeout value.

        Parameters:
        query (StreamingQuery): The Spark Streaming query to monitor.
        timeout (int): The maximum idle time in seconds before stopping the query.

        Returns:
        None
        """
        super().__init__()
        self.query = query
        self.timeout = timeout
        self.last_progress_time = time.time()
        self.daemon = True

    def run(self):
        """
        Run the idle monitoring process.
        This method will continuously check the progress of the query and stop it if no new data is received for the specified timeout.

        Returns:
        None
        """
        while self.query.isActive:
            if time.time() - self.last_progress_time > self.timeout:
                print("No new data for 1 minutes. Stopping the query.")
                self.query.stop()
                os._exit(0)
            time.sleep(10)

    def update_progress_time(self):
        """
        Update the last progress time to the current time.
        This method should be called whenever new data is received by the query.

        Returns:
        None
        """
        self.last_progress_time = time.time()


# MongoDB collection links
crm_mongo_link = mongo_options["mongoUri"] + mongo_options["mongoDbName"] + "." + mongo_options["crmCollectionName"]
device_mongo_link = mongo_options["mongoUri"] + mongo_options["mongoDbName"] + "." + mongo_options["deviceCollectionName"]

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MongoDB_Kafka_Join") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config(conf=conf) \
    .config("spark.mongodb.input.uri", crm_mongo_link) \
    .config("spark.mongodb.input.uri", device_mongo_link) \
    .getOrCreate()

# Kafka broker details
bootstrap_servers = kafka_options["kafkaBootstrapServers"]
topic = kafka_options["kafkaTopicName"]
file_key=kafka_options["kafkaFileName"]

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': kafka_options["kafkaGroupId"],
    'auto.offset.reset': kafka_options["kafkaAutoOffsetReset"]
}

# Revenue schema
rev_schema = StructType([
    StructField("msisdn", StringType(), True),
    StructField("week_number", IntegerType(), True),
    StructField("revenue_usd", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

table_name=sf_options["sfTableName"]
# Initialize Snowflake connector
sf_connector = SnowflakeConnector()

def get_dataframe_from_mongo_kafka():
    """
    This function reads data from MongoDB and Kafka, performs a join operation, and writes the result to a specified table in Snowflake.

    Parameters:
    table_name (str): The name of the table in Snowflake where the result will be written.

    Returns:
    None

    Raises:
    AnalysisException: If there is an error in the Spark DataFrame operations.
    StreamingQueryException: If there is an error in the Spark Streaming query.
    """

    try:
        crm_df = spark.read.format("mongo").option("uri", crm_mongo_link).load()
        device_df = spark.read.format("mongo").option("uri", device_mongo_link).load()

        crm_df.printSchema()

        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
         # Convert the binary values to strings
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
        kafka_df = kafka_df.select(from_json(col("value"), rev_schema).alias("data")).select("data.*")

        kafka_df = kafka_df.filter(kafka_df.msisdn.isNotNull() & kafka_df.week_number.isNotNull() & kafka_df.revenue_usd.isNotNull() & kafka_df.timestamp.isNotNull())
        kafka_df = kafka_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        crm_df = crm_df.drop("_id")
        device_df = device_df.drop("_id")

        # joining logic
        joined_df = kafka_df.join(crm_df, "msisdn", "left").join(device_df, "msisdn", "left")
        joined_df = joined_df.withColumn("revenue_usd", round(col("revenue_usd"), 4))
        joined_df.printSchema()

        query = joined_df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(insert_to_snowflake) \
            .start()
        query = kafka_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        #Initialize and start the idle monitor
        idle_monitor = IdleMonitor(query, 60)
        idle_monitor.start()

        query.awaitTermination()
    except (AnalysisException, StreamingQueryException) as error:
        print(f"Error in Spark job: {error}")
        if 'query' in locals():
            query.stop()
        sys.exit(1)
    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer
        logging.info("Kafka consumer closed.")

def insert_to_snowflake(dataframe,epoch_id):
    """
    This function inserts a DataFrame into a specified table in Snowflake.

    Parameters:
    dataframe (DataFrame): The DataFrame containing the data to be inserted.
    epoch_id (int): The epoch ID of the streaming query. This parameter is not used in this function.

    Returns:
    None

    Raises:
    Exception: If an error occurs while inserting data to Snowflake.

    """
    try:
        sf_connector.write_dataframe(dataframe,table_name)
        logging.info(f"Data inserted into Snowflake table {table_name} successfully")
    except Exception as error:
        logging.error("An error occurred while inserting data to Snowflake: %s",error)
if __name__ == "__main__":
    get_dataframe_from_mongo_kafka()
