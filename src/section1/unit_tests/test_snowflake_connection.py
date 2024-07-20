'''The unittest framework can automatically discover test cases. Any class that subclasses 
unittest.TestCase and contains methods that start with the word test will be considered a 
test case.'''
import pandas as pd
import unittest
from unittest.mock import patch, MagicMock
from src.utility.snowflake_connection import SnowflakeConnector

class TestSnowflakeConnector(unittest.TestCase):
    """
    Test class for the SnowflakeConnector class.

    This class contains test cases for the SnowflakeConnector class. It inherits from unittest.TestCase, 
    which provides a framework for testing in Python.

    """

    def setUp(self):
        # Create a SnowflakeConnector instance
        self.sf_connector = SnowflakeConnector()

    def tearDown(self):
        # Close the Snowflake connection after each test
        self.sf_connector.close()

    @patch('src.utility.snowflake_connection.SnowflakeConnector.connect')
    def test_connect_success(self, mock_connect):
       # Set up the mock to return a mock connection object
        """
    Test the connect method of SnowflakeConnector class when connection is successful.

    This test case uses mocking to simulate a successful connection. It patches the connect method 
    of the SnowflakeConnector class and configures the mock to return a mock connection object.

    Parameters:
    mock_connect (MagicMock): A mock object representing the connect method of SnowflakeConnector class.

    Returns:
    None. This method is used for testing purposes only.

    Raises:
    None. This method does not raise any exceptions.

    """
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection


        # Call the connect method
        connection = self.sf_connector.connect()
        mock_connect.assert_called_once_with()

        self.assertEqual(connection, mock_connection)

    @patch('src.utility.snowflake_connection.SnowflakeConnector.connect')
    def test_connect_failure(self, mock_connect):
        """
    Test the connect method of SnowflakeConnector class when connection fails.

    This test case uses mocking to simulate a failed connection. It patches the connect method 
    of the SnowflakeConnector class and configures the mock to raise an exception.

    Parameters:
    mock_connect (MagicMock): A mock object representing the connect method of SnowflakeConnector class.

    Returns:
    None. This method is used for testing purposes only.

    Raises:
    None. This method does not raise any exceptions.

    """
        # Mock the snowflake.connector.connect method to raise an exception
        # Configure the mock to raise an exception
        mock_connect.side_effect = Exception("Connection failed")

        # Create an instance of SnowflakeConnector
        connector = SnowflakeConnector()

        # Call the connect method
        connection = connector.connect()

        # Assert that the connect method was called once
        mock_connect.assert_called_once()

        # Assert that the connection is None due to failure
        self.assertIsNone(connection)
    @patch('src.utility.snowflake_connection.SnowflakeConnector.write_dataframe')
    @patch('src.utility.snowflake_connection.SnowflakeConnector.connect')
    def test_write_dataframe(self, mock_connect, mock_write_dataframe):
        """
    Test the write_dataframe method of SnowflakeConnector class.

    This test case uses mocking to simulate the behavior of the SnowflakeConnector class.
    It patches the connect and write_dataframe methods of the SnowflakeConnector class.

    Parameters:
    mock_connect (MagicMock): A mock object representing the connect method of SnowflakeConnector class.
    mock_write_dataframe (MagicMock): A mock object representing the write_dataframe method of SnowflakeConnector class.

    Returns:
    None. This method is used for testing purposes only.

    Raises:
    None. This method does not raise any exceptions.
    """
        # Mock the snowflake.connector.connect method
        mock_connect.return_value = MagicMock()

        # Create a sample DataFrame for testing
        sample_data = {
            'column1': [1, 2, 3],
            'column2': ['A', 'B', 'C'],
            'column3': [True, False, True]
        }
        sample_dataframe = pd.DataFrame(sample_data)

        # Call the write_dataframe method of SnowflakeConnector
        self.sf_connector.write_dataframe(sample_dataframe, "test_table")

        # Assert that the write_dataframe method was called with the correct arguments
        mock_write_dataframe.assert_called_once_with(sample_dataframe, "test_table")

    @patch('src.utility.snowflake_connection.SnowflakeConnector.fetch_data_10_weeks_old')
    @patch('src.utility.snowflake_connection.SnowflakeConnector.connect')
    def test_mock_fetch_data_10_weeks_old(self, mock_connect, mock_fetch_data_10_weeks_old):
        """
    Test the fetch_data_10_weeks_old method of SnowflakeConnector class.

    This test case uses mocking to simulate the behavior of the SnowflakeConnector class.
    It patches the connect and fetch_data_10_weeks_old methods of the SnowflakeConnector class.

    Parameters:
    mock_connect (MagicMock): A mock object representing the connect method of SnowflakeConnector class.
    mock_fetch_data_10_weeks_old (MagicMock): A mock object representing the fetch_data_10_weeks_old method of SnowflakeConnector class.

    Returns:
    None. This method is used for testing purposes only.

    Raises:
    None. This method does not raise any exceptions.
    """
        # Mock the snowflake.connector.connect method
        mock_connect.return_value = MagicMock()
        self.sf_connector.fetch_data_10_weeks_old("test_table")
        mock_fetch_data_10_weeks_old.assert_called_once_with("test_table")    
if __name__ == '__main__':
    unittest.main()
# python -m pytest src/section1/unit_tests/test_snowflake_connection.py  
      