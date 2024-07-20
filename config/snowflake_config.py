import os
from dotenv import load_dotenv

# Load environment variables from the snowflake.env file
load_dotenv('snowflake.env')

sf_options = {
    "sfURL": os.getenv('SF_URL'),
    "sfUser": os.getenv('SF_USER'),
    "sfPassword": os.getenv('SF_PASSWORD'),
    "sfDatabase": os.getenv('SF_DATABASE'),
    "sfSchema": os.getenv('SF_SCHEMA'),
    "sfWarehouse": os.getenv('SF_WAREHOUSE'),
    "sfWarehouseSize": os.getenv('SF_WAREHOUSE_SIZE'),
    "sfTableName": os.getenv('SF_TABLE_NAME'),
    "sfCleanTableName": os.getenv('CLEAN_SF_TABLE_NAME'),
}
