""" AWS connection establishment and all aws fucntions"""
from datetime import datetime, timedelta, timezone
from io import StringIO
import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType,IntegerType,DoubleType,TimestampType,StructField,StructType
from config.aws_config import aws_options
# Schema definition
schema = StructType([
    StructField("msisdn", StringType(), True),
    StructField("week_number", IntegerType(), True),
    StructField("revenue_usd", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("gender", StringType(), True),
    StructField("mobile_type", StringType(), True),
    StructField("system_status", StringType(), True),
    StructField("value_segment", StringType(), True),
    StructField("year_of_birth", IntegerType(), True),
    StructField("brand_name", StringType(), True),
    StructField("imei_tac", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("os_name", StringType(), True),
    StructField("os_vendor", StringType(), True)
])

# Set up logging
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

log_file = os.path.join(LOGS_FOLDER, 'aws_connection.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Schema for revenue data
rev_schema = StructType([
    StructField("msisdn", StringType(), True),
    StructField("week_number", IntegerType(), True),
    StructField("revenue_usd", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

class AWSConnector:
    """
    A class for connecting to AWS services and performing operations like uploading 
    to and downloading from S3.
    """
    def __init__(self):
        """
        Initialize AWSConnector instance.
        This method sets up a connection to AWS services using the provided
        credentials and region.
        """
        self.aws_access_key_id = aws_options["awsAccessKeyId"]
        self.aws_secret_access_key = aws_options["awsSecretAccessKey"]
        self.region_name = aws_options["awsRegionName"]
        self.s3_client = None
        self.connect_to_s3()

    def connect_to_s3(self):
        """
        Connect to S3 service.
        This method establishes a connection to AWS S3 using the provided credentials 
        and region.
        """
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            logging.info("Connection to S3 successful")
            return self.s3_client
        except (NoCredentialsError, PartialCredentialsError) as error:
            logging.error(f"Credentials error: {error}")
        except Exception as error:
            logging.error(f"An error occurred while connecting to S3: {error}")

    def fetch_data_from_s3_new(self, bucket_name):
        """
        Fetches data from a specified S3 bucket and returns a DataFrame containing the data.
        This method uses Apache Spark for processing large datasets.

        Parameters:
        bucket_name (str): The name of the S3 bucket.

        Returns:
        pyspark.sql.DataFrame: A DataFrame containing the fetched data.

        Raises:
        NoCredentialsError, PartialCredentialsError: If AWS credentials are not provided or 
        incomplete.
        Exception: If any other error occurs while connecting to S3 or fetching the data.
        """
        try:
            # Initialize the S3 client
            spark = SparkSession.builder \
                .appName("new app") \
                .getOrCreate()

            final_df = spark.createDataFrame([], schema=rev_schema)
            self.s3_client = self.connect_to_s3()
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            for each_object in response['Contents']:
                each_object_name = each_object["Key"]
                last_modified_timestamp = each_object["LastModified"]
                if '/' not in each_object_name and datetime.now(timezone.utc) - timedelta(days=1)\
                      <= last_modified_timestamp <= datetime.now(timezone.utc):
                    iso8601_timestamp = last_modified_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    obj = self.s3_client.get_object(Bucket=bucket_name, Key=each_object_name)
                    csv_content = obj['Body'].read().decode('utf-8')
                    csv_rdd = spark.sparkContext.parallelize(csv_content.split("\n"))
                    df = spark.read.csv(csv_rdd, header=True, inferSchema=True)
                    df = df.withColumn("timestamp", lit(iso8601_timestamp))
                    final_df = final_df.union(df)

            logging.info(f"Successfully read CSV from S3:{bucket_name},key: {each_object_name}")
            return final_df
        except (NoCredentialsError, PartialCredentialsError) as error:
            logging.error(f"Credentials error: {error}")
        except Exception as error:
            logging.error(f"An error occurred while connecting to S3: {error}")

    def fetch_data_from_s3(self, bucket_name, file_key):
        """
        Fetches data from a specified S3 bucket and file key.

        Parameters:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key of the file within the bucket.

        Returns:
        pandas.DataFrame: A DataFrame containing the fetched data.

        Raises:
        NoCredentialsError, PartialCredentialsError: If AWS credentials
        are not provided or incomplete.Exception:If any other error occurs while
        connecting to S3 or fetching the data.
        """  
        try:
            self.s3_client = self.connect_to_s3()
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            response = self.s3_client.head_object(Bucket=bucket_name, Key=file_key)
            last_modified_timestamp = response['ResponseMetadata']['HTTPHeaders']['last-modified']
            last_modified_dt = datetime.strptime(last_modified_timestamp, '%a, %d %b %Y %H:%M:%S %Z')
            iso8601_timestamp = last_modified_dt.strftime('%Y-%m-%d %H:%M:%S')
            result_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            result_df['timestamp'] = iso8601_timestamp
            logging.info(f"Successfully read CSV from S3 bucket: {bucket_name}, key: {file_key}")
            return result_df
        except (NoCredentialsError, PartialCredentialsError) as error:
            logging.error("Credentials error: %s",error)
        except Exception as error:
            logging.error("An error occurred while connecting to S3: %s",error)

    def push_data_to_s3(self, data, bucket_name, folder_path):
        """
        This function uploads a given data to a specified S3 bucket and folder.

        Parameters:
        data (list of lists): The data to be uploaded. Each inner list represents a row in the CSV file.
        bucket_name (str): The name of the S3 bucket where the data will be uploaded.
        folder_path (str): The path within the bucket where the data will be uploaded.

        Returns:
        None

        Raises:
        NoCredentialsError, PartialCredentialsError: If AWS credentials are not provided or incomplete.
        Exception: If any other error occurs while connecting to S3 or uploading the data.
        """
        try:
            self.s3_client = self.connect_to_s3()
            file_name = f"data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            with open(file_name, 'w') as file:
                for row in data:
                    file.write(','.join(map(str, row)) + '\n')
            # Upload the file to S3
            self.s3_client.upload_file(file_name, bucket_name, os.path.join(folder_path, file_name))
            # Remove the temporary CSV file
            os.remove(file_name)
            logger.info("Data uploaded to S3 successfully")
        except (NoCredentialsError, PartialCredentialsError) as error:
            logging.error("Credentials error: %s",error)
        except Exception as error:
            logging.error("An error occurred while connecting to S3 %s ",error)

if __name__ == "__main__":
    aws_connector = AWSConnector()
