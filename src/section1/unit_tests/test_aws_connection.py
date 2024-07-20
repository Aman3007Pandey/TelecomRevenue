""" Test for aws connection setup"""
import unittest
from unittest.mock import patch, MagicMock
from src.utility.aws_connection import AWSConnector


class TestAwsConnector(unittest.TestCase):
    """
    Test class for the AWSConnector class.

    This class contains test methods to verify the functionality of the AWSConnector class.
    The test methods use Python's unittest framework and the unittest.mock module to mock
    the AWSConnector class's methods and assert their behavior.
    """

    def setUp(self):
        # Create a SnowflakeConnector instance
        self.aws_connector = AWSConnector()
    @patch('src.utility.aws_connection.AWSConnector.connect_to_s3')
    def test_connect_success(self, mock_connect):
       # Set up the mock to return a mock connection object
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        # Call the connect method
        connection = self.aws_connector.connect_to_s3()
        mock_connect.assert_called_once_with()
        self.assertEqual(connection, mock_connection)

    @patch('src.utility.aws_connection.AWSConnector.fetch_data_from_s3')
    @patch('src.utility.aws_connection.AWSConnector.connect_to_s3')
    def test_fetch_data_from_s3(self, mock_connect, mock_fetch_data_from_s3):
        """
    Test method to verify the functionality of fetching data from AWS S3.

    Parameters:
    mock_connect (MagicMock): A mock object representing the AWSConnector.connect_to_s3 method.
    mock_fetch_data_from_s3 (MagicMock): A mock object representing the AWSConnector.fetch_data_from_s3 method.

    Returns:
    None

    This method sets up the necessary mocks, calls the fetch_data_from_s3 method of AWSConnector,
    and asserts that the method was called with the correct arguments.
    """
        # Mock the snowflake.connector.connect method
        mock_connect.return_value = MagicMock()
        # Call the write_dataframe method of SnowflakeConnector
        self.aws_connector.fetch_data_from_s3("bucket_name", "file_key")

        # Assert that the write_dataframe method was called with the correct arguments
        mock_fetch_data_from_s3.assert_called_once_with("bucket_name", "file_key")

    @patch('src.utility.aws_connection.AWSConnector.push_data_to_s3')
    @patch('src.utility.aws_connection.AWSConnector.connect_to_s3')
    def test_push_data_to_s3(self,mock_connect,mock_push_data_to_s3):
        """
    Test method to verify the functionality of pushing data to AWS S3.

    Parameters:
    mock_connect (MagicMock): A mock object representing the AWSConnector.connect_to_s3 method.
    mock_push_data_to_s3 (MagicMock): A mock object representing the AWSConnector.push_data_to_s3 method.

    Returns:
    None

    This method sets up the necessary mocks, calls the push_data_to_s3 method of AWSConnector,
    and asserts that the method was called with the correct arguments.
    """
         # Mock the snowflake.connector.connect method
        mock_connect.return_value = MagicMock()
        # Call the write_dataframe method of SnowflakeConnector
        self.aws_connector.push_data_to_s3("data", "bucket_name","folder_path")

        # Assert that the write_dataframe method was called with the correct arguments
        mock_push_data_to_s3.assert_called_once_with("data","bucket_name", "folder_path")

if __name__ == '__main__':
    unittest.main()
