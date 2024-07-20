import logging
import os
import snowflake.connector
from config.snowflake_config import sf_options

# Create a logs folder if it doesn't exist
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

# Configure logging to write to a file in the logs folder
log_file = os.path.join(LOGS_FOLDER, 'consumer.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class SnowflakeConnector:
    """
    A class for connecting to Snowflake database and performing operations
    like writing DataFrame and fetching data.
    """

    def __init__(self):
        """
        Initialize SnowflakeConnector instance.

        This method establishes a connection to Snowflake database using
        the provided configuration options.
        If the connection is successful, it sets the connection
        object to the 'elf.connection' attribute.
        If the connection fails, it logs the error and sets 'elf.connection' to None.
        """
        self.connection = None
        self.connect()
    def connect(self):
        """
        Connect to Snowflake database.

        This method attempts to establish a connection to Snowflake
        database using the provided configuration options.
        If the connection is successful, it logs the success
        message and returns the connection object.
        If the connection fails, it logs the error and returns None.
        """
        try:
            self.connection = snowflake.connector.connect(
                account=sf_options["sfURL"].split('.snowflakecomputing.com')[0].split('//')[1],
                user=sf_options["sfUser"],
                password=sf_options["sfPassword"],
                database=sf_options["sfDatabase"],
                schema=sf_options["sfSchema"],
                warehouse=sf_options["sfWarehouse"]
            )
            logging.info("Connection to Snowflake successful")
            return self.connection
        except Exception as error:
            logging.error("An error occurred: %s",error)
            self.connection = None
            return None
    
    def close(self):
        """
        Close the connection to Snowflake database.

        This method checks if there is an 
        active connection to Snowflake database.
        If there is an active connection, it closes the
        connection and logs the success message.
        If there is no active connection, it logs a warning message.
        """
        if self.connection:
            self.connection.close()
            logging.info("Connection to Snowflake closed")
        else:
            logging.warning("No active connection to close")

    def write_dataframe(self, dataframe, table_name):
        """
        Write a DataFrame to a specified table in Snowflake database.

        This method checks if there is an active connection
        to Snowflake database.
        If there is an active connection, it writes the
        DataFrame to the specified table using the provided table name.
        If there is no active connection, it logs a warning message.
        """
        if self.connection:
            try:
                dataframe.write \
                   .format("snowflake")  \
                   .options(**sf_options) \
                   .option("dbtable", table_name) \
                   .mode("append") \
                   .save()
                logging.info(f"Data written to {table_name} successfully")
            except Exception as error:
                logging.error("An error occurred while writing the DataFrame to Snowflake: %s" ,error)
        else:
            logging.warning("No active connection to Snowflake")

    
    def clean_my_table(self, staged_table_name,clean_table_name):
        if self.connection:
            try:
            
                query=f"""create or replace table {clean_table_name} as with aggregated_data
                  as (select msisdn, week_number,timestamp,imei_tac, sum(revenue_usd) as revenue_usd,
                    max(brand_name) as brand_name,
        max(model_name) as model_name,
        max(os_name) as os_name,
        max(os_vendor) as os_vendor,
        max(gender) as gender,
        max(year_of_birth) as year_of_birth,
        max(system_status) as system_status,
        max(mobile_type) as mobile_type,
        max(value_segment) as value_segment
    from {staged_table_name} group by 
        msisdn, 
        week_number, 
        timestamp,
        imei_tac
)
select
    msisdn,
    week_number,
    revenue_usd,
    timestamp,
    imei_tac,
    brand_name,
    model_name,
    os_name,
    os_vendor,
    gender,
    year_of_birth,
    system_status,
    mobile_type,
    value_segment
from aggregated_data;"""
                # Execute the query to fetch data older than 10 weeks
                cursor = self.connection.cursor()
                cursor.execute(query)
                # Close cursor
                cursor.close()
                logging.info(f"{clean_table_name} Has been successfully updated !")
            except Exception as error:
                logging.error("An error occurred while fetching data: %s",error)
        else:
            logging.warning("No active connection to Snowflake")
    def fetch_data_10_weeks_old(self,table_name):
        """
    Fetch data from a specified table in Snowflake database that is older than 10 weeks.

    Parameters:
    table_name (str): The name of the table from which to fetch data.

    Returns:
    list: A list of tuples containing the fetched data. Each tuple represents a row in the table.

    Raises:
    Exception: If an error occurs while fetching data from the database.
    """
        if self.connection:
            try:
                VALID_TABLE_NAMES = ["JOINED_TABLE", "STAGED_TABLE"] 
                if table_name not in VALID_TABLE_NAMES:
                    raise ValueError("Invalid table name provided.")
                query = "SELECT * FROM JOINED_TABLE WHERE week_number = %s"
                cursor = self.connection.cursor()
                cursor.execute(query, (23))
                data=cursor.fetchall()
                cursor.close()
                logging.info(f"Data archived from {table_name} to S3-Glacier Instant Retrival !")
                return data
            except Exception as error:
                logging.error("An error occurred while fetching data: %s",error)
                return None
        else:
            logging.warning("No active connection to Snowflake")
            return None

if __name__ == "__main__":
    sf_connector = SnowflakeConnector()
    sf_connector.close()
