""" Selects a particular week according to the calendar"""
from datetime import datetime
import logging
import os

# Create a logs folder if it doesn't exist
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

# Configure logging
logging.basicConfig(filename=os.path.join(LOGS_FOLDER, 'producer.log'),level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_current_week_number():
    """
    This function retrieves the current week number based on the ISO 8601 standard.

    Parameters:
    None

    Returns:
    int: The current week number. If an error occurs during the process, returns None.

    Raises:
    None """
    try:
        now = datetime.now()
        week_number = now.isocalendar()[1]
        print("Week Number: %s",week_number)
        return week_number
    except Exception as error:
        logger.error(f"Error getting current week number: {error}", exc_info=True)
        return None

# Example usage
if __name__ == "__main__": 
    current_week_number = get_current_week_number()
    if current_week_number is not None:
        print("Current Week Number: %s",current_week_number)
