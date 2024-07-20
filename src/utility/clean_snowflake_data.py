""" Module cleaning data in snowflake stage ."""
import logging
import os
from src.utility.snowflake_connection import SnowflakeConnector
from config.snowflake_config import sf_options


# Set up logging
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

log_file = os.path.join(LOGS_FOLDER, 'snowflake_clean.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)  

sf_connector=SnowflakeConnector()

if __name__ == "__main__":
    try:
        # Fetch data from Snowflake
        sf_connector.clean_my_table(sf_options['sfTableName'],sf_options['sfCleanTableName'])
        logger.info("Cleaned successfully done on snowflake table")
    except Exception as error:
        logger.error("An error occurred: %s " ,error)
        