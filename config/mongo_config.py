import os
from dotenv import load_dotenv

# Load environment variables from mongo.env
load_dotenv('mongodb.env')

mongo_options = {
    "mongoUri": os.getenv('MONGO_URI'),
    "mongoDbName": os.getenv('MONGO_DB_NAME'),
    "crmCollectionName": os.getenv('CRM_COLLECTION_NAME'),
    "deviceCollectionName": os.getenv('DEVICE_COLLECTION_NAME'),
    "smallCrmCollectionName":os.getenv('SMALL_CRM_COLLECTION_NAME'),
    "smallDeviceCollectionName":os.getenv('SMALL_DEVICE_COLLECTION_NAME'),
    "cleanCrmCollectionName":os.getenv('CLEAN_CRM_COLLECTION_NAME'),
    "cleanDeviceCollectionName":os.getenv('CLEANL_DEVICE_COLLECTION_NAME'),
}


