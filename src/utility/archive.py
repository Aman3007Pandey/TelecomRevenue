""" Module for data transfer operations between Snowflake and AWS S3-Glacier ."""
import os
import logging
from src.utility.snowflake_connection import SnowflakeConnector
from src.utility.aws_connection import AWSConnector
from config.aws_config import aws_options
from config.snowflake_config import sf_options

# Set up logging
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

log_file = os.path.join(LOGS_FOLDER, 'archive.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Snowflake connection parameters
sf_connector = SnowflakeConnector()
# AWS connection parameters
aws_connector=AWSConnector()
# S3 connection parameters
s3_bucket_name = aws_options["awsS3BucketName"]
s3_folder_path = aws_options["awsS3ArchiveFolder"]
# Snowflake connection parameters
table_name=sf_options["sfCleanTableName"]


if __name__ == "__main__":
    try:
        # Fetch data from Snowflake
        data = sf_connector.fetch_data_10_weeks_old(table_name)
        if data:
            # Upload data to S3
            aws_connector.push_data_to_s3(data,s3_bucket_name,s3_folder_path)
        else:
            logger.warning("No data fetched from Snowflake")
    except Exception as e:
        logger.error("An error occurred: %s", e)
#python3 -m  pylint src.utility.archive
             