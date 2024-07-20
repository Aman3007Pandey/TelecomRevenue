""" Monog connection establishment and all mongo fucntions"""
import os
import logging
from pymongo import MongoClient
from config.mongo_config import mongo_options

# Create a logs folder if it doesn't exist
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

# Configure logging to write to a file in the logs folder
log_file = os.path.join(LOGS_FOLDER, 'mongodb_insertion.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class MongoDBConnector:
    def __init__(self):
        """
    Initialize MongoDBConnector instance.

    This method establishes a connection to MongoDB using the provided configuration options.
    If the connection is successful, it sets the client and db attributes accordingly.
    If an error occurs during the connection process, it logs the error and sets both
      client and db attributes to None.

    Parameters:
    None

    Returns:
    None
    """
        self.client = None
        self.database = None
        self.connect()
    
    def connect(self):
        """
    Establish a connection to MongoDB using the provided configuration options.

    This method attempts to create a MongoClient instance using the provided
      mongoUri from the mongo_options dictionary.
    If the connection is successful, it sets the client and db attributes accordingly.
    If an error occurs during the connection process, it logs the error and
      sets both client and db attributes to None.

    Parameters:
    None

    Returns:
    None
    """
        try:
            self.client = MongoClient(mongo_options["mongoUri"])
            self.database = self.client[mongo_options["mongoDbName"]]
            logging.info("Connection to MongoDB successful")
        except Exception as error:
            logging.error("An error occurred: %s",error)
            self.client = None
            self.database = None
    def insert_many(self, collection_name, data):
        """
    Inserts multiple documents into a specified MongoDB collection.

    This method checks if a connection to MongoDB is active. If a connection exists,
    it attempts to insert the provided data into the specified collection. If successful,
    it logs a success message. If an error occurs during the insertion process, it logs
    an error message. If no active connection to MongoDB exists, it logs a warning message.

    Parameters:
    collection_name (str): The name of the MongoDB collection into which
      the data will be inserted.
    data (list): A list of dictionaries representing the documents to be inserted.

    Returns:
    None
    """
        if self.database is not None:
            try:
                collection = self.database[collection_name]
                collection.insert_many(data)
                logging.info(f"Data inserted into {collection_name} successfully")
            except Exception as error:
                logging.error("An error occurred while inserting data: % s",error)
        else:
            logging.warning("No active connection to MongoDB")
    def close(self):
        """
    Closes the active MongoDB connection.

    This method checks if a connection to MongoDB is active. If a connection exists,
    it attempts to close the connection using the MongoClient's close method. If successful,
    it logs a success message. If no active connection to MongoDB exist
    s, it logs a warning message.

    Parameters:
    None

    Returns:
    None
    """
        if self.client:
            self.client.close()
            logging.info("Connection to MongoDB closed")
        else:
            logging.warning("No active connection to close")

if __name__ == "__main__":
    mongo_connector = MongoDBConnector()
    mongo_connector.close()
