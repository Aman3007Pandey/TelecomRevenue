""" Convert the crm1.csv and device1.csv data into relevant json and store 
the data in a NoSQL DB of your choice e.g. MongoDB, Cassandra, CouchDB, HBase etc. 
As a part of analysis perform some data analysis to check the data quality like duplicates, 
missing column values i.e. blanks, nulls etc. 
As part of the project submission mentions why a particular technology was chosen.
Analyze and explain if you should do these checks before ingesting the data in
 NoSQL or after ingesting
"""

from io import StringIO
import logging
import json
import boto3
import pandas as pd
from config.mongo_config import mongo_options
from config.aws_config import aws_options
from src.utility.mongo_connection import MongoDBConnector


def read_csv_from_s3(file_key):
    """
    This function reads a CSV file from an S3 bucket and returns it as a pandas DataFrame.
    It also adds a 'timestamp' column to the DataFrame with the last modified
      timestamp of the file.

    Parameters:
    bucket_name (str): The name of the S3 bucket.
    file_key (str): The key of the CSV file in the S3 bucket.

    Returns:
    pandas.DataFrame: The DataFrame containing the CSV data with a 'timestamp' column.
    None: If an error occurs while reading the CSV file from S3.

    Raises:
    Exception: If any error occurs while reading the CSV file from S3.
    """
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_options["awsAccessKeyId"],
            aws_secret_access_key=aws_options["awsSecretAccessKey"],
            region_name=aws_options["awsRegionName"]
        )
        bucket_name = aws_options["awsS3BucketName"]
        # Get the CSV file from S3 and load it into a pandas DataFrame
        obj = s3.get_object(Bucket=aws_options["awsS3BucketName"], Key=file_key)
        result_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        logging.info(f"Successfully read CSV from S3 bucket: {bucket_name}, key: {file_key}")
        return result_df
    except Exception as error:
        logging.error(f"Error reading CSV from S3: {error}", exc_info=True)
        return None


def convert_csv_to_json(crm_key, device_key):
    """
    Reads CRM and Device CSV files and converts them to JSON format using PySpark.

    :param crm_csv: Path to the CRM CSV file.
    :param device_csv: Path to the Device CSV file.
    :return: Tuple containing two JSON strings, one for CRM data and one for Device data.
    :rtype: tuple (str, str)
    """
    try:
        # Read the CSV files from S3 into DataFrames using Pandas

        crm_df = read_csv_from_s3(crm_key)
        device_df = read_csv_from_s3(device_key)

        if crm_df is None or device_df is None:
            raise Exception("Failed to read one or both CSV files from S3.")

        # Convert DataFrames to JSON strings
        crm_json_string = crm_df.to_json(orient='records')
        device_json_string = device_df.to_json(orient='records')

        # Return the JSON strings
        return crm_json_string, device_json_string

    except Exception as errors:
        logging.error("An error occurred during CSV to JSON conversion: %s",errors)
        raise

if __name__ == "__main__":
    try:
    # Example usage
        #print(aws_options)
        # CRM_KEY = 'crm_cleaned.csv'
        # DEVICE_KEY = 'device_cleaned.csv'
        CRM_KEY = 'smallcrm1.csv'
        DEVICE_KEY = 'smalldevice1.csv'
        crm_json_str, device_json_str = convert_csv_to_json(CRM_KEY, DEVICE_KEY)
        crm_data = json.loads(crm_json_str)
        device_data = json.loads(device_json_str)
        mongo_connector = MongoDBConnector()
        mongo_connector.insert_many(mongo_options["crmCollectionName"], crm_data)
        mongo_connector.insert_many(mongo_options["deviceCollectionName"], device_data)
        mongo_connector.close()
        logging.info("Data insertion to MongoDB successful.")
    except Exception as error:
        logging.error("An error occurred during data insertion to MongoDB: %s",error)
