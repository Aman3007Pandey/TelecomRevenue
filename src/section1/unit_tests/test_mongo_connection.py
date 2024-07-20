import unittest
from unittest.mock import patch, MagicMock
from src.utility.mongo_connection import MongoDBConnector
from config.mongo_config import mongo_options
class TestMongoDBConnector(unittest.TestCase):
    """
    Test class for the MONGO Connector class.

    This class contains test methods to verify the functionality of the MONGO Connector class.
    The test methods use Python's unittest framework and the unittest.mock module to mock
    the MONGO Connector class's methods and assert their behavior.
    """
    @patch('src.utility.mongo_connection.MongoClient')
    def test_connect_success(self, mock_mongo_client):
        """
    Test case to verify successful connection to MongoDB.

    Parameters:
    mock_mongo_client (MagicMock): A mock object for MongoClient.

    Returns:
    None
    """
        # Mock MongoClient
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        # Create MongoDBConnector instance
        connector = MongoDBConnector()

        # Assert MongoClient is called with the correct URI
        expected_uri = mongo_options["mongoUri"]
        mock_mongo_client.assert_called_once_with(expected_uri)

        # Assert db attribute is set correctly
        self.assertIsNotNone(connector.database)

    @patch('src.utility.mongo_connection.MongoClient')
    def test_connect_failure(self, mock_mongo_client):
        """
    Test case to verify the failure scenario when connecting to MongoDB.
    Parameters:
    mock_mongo_client (MagicMock): A mock object for MongoClient.
    Returns:
    None
    Raises:
    Exception: If the MongoClient raises an exception during the connection process.
    This test case sets up a mock MongoClient to raise an exception. It then creates an instance of MongoDBConnector.
    Afterwards, it asserts that the client and db attributes of the connector are None, indicating a failed connection.
    """

        # Mock MongoClient to raise an exception
        mock_mongo_client.side_effect = Exception('Mocked exception')

        # Create MongoDBConnector instance
        connector = MongoDBConnector()

        # Assert client and db attributes are None
        self.assertIsNone(connector.client)
        self.assertIsNone(connector.database)

if __name__ == '__main__':
    unittest.main()
