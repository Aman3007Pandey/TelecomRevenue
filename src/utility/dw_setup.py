""" SNOWFLAKE Credentials """
import logging
import os
from config.snowflake_config import sf_options
from utility.snowflake_connection import SnowflakeConnector


# Create a logs folder if it doesn't exist
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

# Configure logging to write to a file in the logs folder
log_file = os.path.join(LOGS_FOLDER, 'mongodb_insertion.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
class SnowflakeSetupExtended(SnowflakeConnector):
    """ A class used to extend the functionality of the SnowflakeConnector class.
    This class provides methods for creating a warehouse, database, and schema in Snowflake.

    Attributes:
    None

    Methods:
    create_warehouse(warehouse_name, size="XSMALL"): Creates a new warehouse in Snowflake.
    create_database(database_name): Creates a new database in Snowflake.
    create_schema(database_name, schema_name): Creates a new schema in the
      specified database in Snowflake."""
    def __init__(self):
        super().__init__()

    def create_warehouse(self, warehouse_name, size="XSMALL"):
        """
    Creates a new warehouse in Snowflake with the given name and size.

    Parameters:
    warehouse_name (str): The name of the warehouse to be created.
    size (str, optional): The size of the warehouse. Defaults to "XSMALL".
        Accepts short forms (XS, S, M, L, XL, XXL, XXXL) which will be
          mapped to their full names.

    Returns:
    None

    Raises:
    Exception: If an error occurs while creating the warehouse.
    """
        # Map short forms to full names
        size_map = {
            "XS": "EXTRA_SMALL",
            "S": "SMALL",
            "M": "MEDIUM",
            "L": "LARGE",
            "XL": "EXTRA_LARGE",
            "XXL": "XXLARGE",
            "XXXL": "XXXLARGE"
        }
        # Check if the provided size is a short form
        if size.upper() in size_map:
            size = size_map[size.upper()]

        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name} \
                                   WITH WAREHOUSE_SIZE = '{size}'")
                    logging.info(f"Warehouse {warehouse_name} created successfully wit \
                                 h size {size}")
            except Exception as error:
                logging.error("An error occurred while creating the warehouse: %s",error)
        else:
            logging.warning("No active connection to Snowflake")

    def create_database(self, database_name):
        """
    Creates a new database in Snowflake with the given name.

    Parameters:
    database_name (str): The name of the database to be created.

    Returns:
    None

    Raises:
    Exception: If an error occurs while creating the database.

    Note:
    This method assumes that a valid connection to Snowflake has been established.
    If no active connection exists, a warning message will be logged.
    """
        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                    logging.info(f"Database {database_name} created successfully")
            except Exception as error:
                logging.error("An error occurred while creating the database: %s",error)
        else:
            logging.warning("No active connection to Snowflake")

    def create_schema(self, database_name, schema_name):
        """Creates a new schema in the specified database in Snowflake.

    Parameters:
    database_name (str): The name of the database where the schema will be created.
    schema_name (str): The name of the schema to be created.

    Returns:
    None

    Raises:
    Exception: If an error occurs while creating the schema.

    Note:
    This method assumes that a valid connection to Snowflake has been established.
    If no active connection exists, a warning message will be logged.
    """
        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"USE DATABASE {database_name}")
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    logging.info(f"Schema {schema_name} created successfully in \
                                  database {database_name}")
            except Exception as error:
                logging.error("An error occurred while creating the schema: % s",error)
        else:
            logging.warning("No active connection to Snowflake")

if __name__ == "__main__":
    try:
        sf_setup = SnowflakeSetupExtended()
        # Create a warehouse, database, and schema
        sf_setup.create_warehouse(sf_options["sfWarehouse"], size=sf_options["sfWarehouseSize"])
        sf_setup.create_database(sf_options["sfDatabase"])
        sf_setup.create_schema(sf_options["sfDatabase"], sf_options["sfSchema"])
        sf_setup.close()

        logging.info("Snowflake setup completed successfully.")
    except Exception as error:
        logging.error("An error occurred during Snowflake setup: %s",error)
