import os
from dotenv import load_dotenv

# Load Kafka environment variables from kafka.env file
load_dotenv('aws.env')

aws_options = {
    "awsAccessKeyId": os.getenv("AWS_ACCESS_KEY_ID"),
    "awsSecretAccessKey": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "awsRegionName": os.getenv("AWS_REGION_NAME"),
    "awsS3BucketName": os.getenv("AWS_S3_BUCKET_NAME"),
    "awsS3ArchiveFolder":os.getenv("AWS_S3_ARCHIVE_FOLDER")
}