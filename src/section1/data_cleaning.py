""" Cleaning logic of data stored in mongo using SPARK """
import os
import logging
from pyspark.sql import SparkSession , Window
from pyspark.sql.functions import count, isnull, row_number , col, udf ,desc,lead, lag, when, max as max_
from pyspark.sql.types import StringType
import boto3
import jellyfish
from config.aws_config import aws_options


# Create a logs folder if it doesn't exist
LOGS_FOLDER = 'logs/'
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

# Configure logging to write to a file in the logs folder
log_file = os.path.join(LOGS_FOLDER, 'data_cleaning.log')
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    # Create a Spark session with AWS credentials for S3 access and Hadoop configuration
    spark = SparkSession.builder \
        .appName("CSV to JSON") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.hadoop.fs.s3a.access.key", aws_options["awsAccessKeyId"]) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_options["awsSecretAccessKey"]) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_options['awsRegionName']}. \
                amazonaws.com") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .getOrCreate()
    return spark

def read_csv_from_s3_spark(spark, file_key):
    try:
        # Read the CSV file from S3 directly into a Spark DataFrame
        spark_df = spark.read.csv(f"s3a://{aws_options['awsS3BucketName']}/{file_key}", \
                                   header=True, inferSchema=True)

        logging.info(f"Successfully read CSV from S3 bucket: \
                      {aws_options['awsS3BucketName']}, key: {file_key}")
        return spark_df
    except Exception as error:
        logging.error(f"Error reading CSV from S3: {error}", exc_info=True)
        return None

spark = create_spark_session()

CRM_KEY = 'crm1.csv'
DEV_KEY = 'device1.csv'

crm_data =read_csv_from_s3_spark(spark, CRM_KEY)
device_data = read_csv_from_s3_spark(spark, DEV_KEY)
# First return the different number of repetitions and then corresponding
# count of msisdn values repeating for that time
msisdn_counts = crm_data.groupBy('msisdn').agg(count('msisdn').alias('count'))

# Group by the count of repetitions and count the number of msisdn for each count
repetition_counts = msisdn_counts.groupBy('count').agg(count('msisdn').alias('num_msisdn'))

# Order by count and show the results
repetition_counts.orderBy('count').show()

# List of valid gender values
gender_values = ['male', 'female']

def fuzzy_match_with_threshold(value, threshold):
    """
    This function performs fuzzy matching on a given value with a
      list of predefined terms.
    It uses the Jaro-Winkler similarity metric to calculate the 
    similarity scores between the value and each term.
    If the similarity score is above the given threshold, the function
      returns the corresponding term.
    If no match is found, it returns None.

    Parameters:
    value (str): The value to be matched.
    threshold (float): The minimum similarity score required for a match.

    Returns:
    str: The matched term or None if no match is found.
    """
    if value is None:
        return None
    scores = [jellyfish.jaro_winkler_similarity(value.lower(), term) for term in gender_values]
    max_index = scores.index(max(scores))
    if scores[max_index] >= threshold:
        return gender_values[max_index].capitalize()
    return None

def get_fuzzy_match_udf(threshold):
    """
    This function returns a User Defined Function (UDF) for fuzzy matching gender values.
    The UDF uses the Jaro-Winkler similarity metric to match the input
      gender value with predefined gender terms.
    If the similarity score is above the given threshold, the corresponding term is returned.
    If no match is found, None is returned.

    Parameters:
    threshold (float): The minimum similarity score required for a match.

    Returns:
    pyspark.sql.functions.udf: A UDF for fuzzy matching gender values.
    """
    return udf(lambda x: fuzzy_match_with_threshold(x, threshold), StringType())

# Testing for no match case
def test_fuzzy_match_with_threshold_no_match():
    """
    This function tests the fuzzy_match_with_threshold function when there is no match.

    Parameters:
    None

    Returns:
    None
    """
    threshold = 0.0
    value = "Other"
    result = fuzzy_match_with_threshold(value, threshold)
    print(result)
    assert result is None, f"Expected None, but got {result}"

test_fuzzy_match_with_threshold_no_match()

# Testing for exact match case
def test_fuzzy_match_with_threshold_exact_match():
    """
    This function tests the fuzzy_match_with_threshold function with an exact match.

    Parameters:
    None

    Returns:
    None

    Raises:
    None
    """
    threshold = 1.0
    value = "Mal"
    result = fuzzy_match_with_threshold(value, threshold)
    print(result)
    assert result == "Male", f"Expected Male, but got {result}"

test_fuzzy_match_with_threshold_exact_match()

def test_fuzzy_match_with_threshold_empty_string_low_threshold():
    """
    This function tests the fuzzy_match_with_threshold function with an empty
      string and a low threshold.

    Parameters:
    None

    Returns:
    None

    Raises:
    None
    """
    threshold = 0.0
    value = ""
    result = fuzzy_match_with_threshold(value, threshold)
    print(result)
    assert result is None, f"Expected None, but got {result}"

test_fuzzy_match_with_threshold_empty_string_low_threshold()

# Evaluate threshold for fuzzy matching
def evaluate_threshold(threshold):
    """
    This function evaluates the effectiveness of the fuzzy matching algorithm
      for cleaning the gender column in the CRM data.
    It uses a given threshold to determine the minimum similarity
      score required for a match.
    The function applies the fuzzy matching algorithm to the gender
      column, groups the results by the cleaned gender values,
    and displays the distinct gender values and their corresponding cleaned values.
    It also shows the count of each cleaned gender value.

    Parameters:
    threshold (float): The minimum similarity score required for a match.

    Returns:
    None
    """
    fuzzy_match_udf = get_fuzzy_match_udf(threshold)
    temp_df = crm_data.withColumn('gender_cleaned', fuzzy_match_udf(col('gender')))
    res_df = temp_df.groupBy('gender_cleaned').count().orderBy('count', ascending=False)
    temp_df.select('gender', 'gender_cleaned').distinct().show(100, truncate=False)
    res_df.show()

evaluate_threshold(threshold=0.75)

# Apply fuzzy matching and clean gender column
fuzzy_match_udf = get_fuzzy_match_udf(threshold=0.75)
crm_data_cleaned = crm_data.withColumn('gender_cleaned', fuzzy_match_udf(col('gender')))
crm_data_cleaned = crm_data_cleaned.withColumn('gender', col('gender_cleaned')).drop('gender_cleaned')

# Filter out unrealistic year_of_birth values
crm_data_cleaned = crm_data_cleaned.filter((col('year_of_birth') < 2024) | (col('year_of_birth').isNull()))

# Define window specification for ordering
window_spec = Window.partitionBy("msisdn").orderBy("year_of_birth", "gender", "mobile_type", "system_status", "value_segment")

# Add columns to check for adjacent active/suspend pairs within the same SIM card
df = crm_data_cleaned
df = df.withColumn("next_status", lead("system_status").over(window_spec)) \
    .withColumn("next_yob", lead("year_of_birth").over(window_spec)) \
    .withColumn("next_gender", lead("gender").over(window_spec)) \
    .withColumn("prev_status", lag("system_status").over(window_spec)) \
    .withColumn("prev_yob", lag("year_of_birth").over(window_spec)) \
    .withColumn("prev_gender", lag("gender").over(window_spec))

# Create a flag to identify pairs to be filtered out
df = df.withColumn("pair_flag", when(
    (col("system_status") == "ACTIVE") & (col("next_status") == "SUSPEND") & (col("year_of_birth") == col("next_yob")) & (col("gender") == col("next_gender")), 1
).when(
    (col("system_status") == "SUSPEND") & (col("prev_status") == "ACTIVE") & (col("year_of_birth") == col("prev_yob")) & (col("gender") == col("prev_gender")), 1
).when(
    (col("system_status") == "ACTIVE") & (col("next_status") == "IDLE") & (col("year_of_birth") == col("next_yob")) & (col("gender") == col("next_gender")), 1
).otherwise(0))

# Filter out pairs and prepare for final selection
filtered_df = df.filter(col("pair_flag") == 0).drop("pair_flag", "next_status", "next_yob", "next_gender", "prev_status", "prev_yob", "prev_gender")

# Handle deactivation as end of life cycle
window_spec_rev = Window.partitionBy("msisdn").orderBy(col("year_of_birth").desc())
filtered_df = filtered_df.withColumn("deactivate_flag", when(col("system_status") == "DEACTIVATE", 1).otherwise(0)) \
    .withColumn("max_deactivate_flag", max_("deactivate_flag").over(window_spec_rev)) \
    .filter(col("max_deactivate_flag") == 0) \
    .drop("deactivate_flag", "max_deactivate_flag")

# Count non-null values in columns
filtered_df = filtered_df.withColumn("non_null_count", sum(
    when(col(c).isNotNull(), 1).otherwise(0) for c in ["gender", "year_of_birth", "system_status", "mobile_type", "value_segment"]
))

# Prioritize records based on status and non-null count
priority_window_spec = Window.partitionBy("msisdn").orderBy(
    when(col("system_status") == "ACTIVE", 1)
    .when(col("system_status") == "IDLE", 2)
    .when(col("system_status") == "SUSPEND", 3)
    .when(col("system_status") == "DEACTIVATE", 4).asc(),
    col("non_null_count").desc()
)

priority_df = filtered_df.withColumn("priority", row_number().over(priority_window_spec))

# Select records with the highest priority
final_df = priority_df.filter(col("priority") == 1).select(
    "msisdn", "gender", "year_of_birth", "system_status", "mobile_type", "value_segment"
).distinct()

# Show rows with msisdn and their duplicate counts
crm_data_cleaned.groupBy('msisdn').count().orderBy("count", ascending=False).show(100, truncate=False)

# Show final null counts for cleaned CRM data
final_df.select([count(when(isnull(c), c)).alias(c) for c in final_df.columns]).show(truncate=False)

# Assign cleaned CRM data to crm_final
crm_final = final_df

# Show initial null counts for device data
device_data.select([count(when(isnull(c), c)).alias(c) for c in device_data.columns]).show(truncate=False)

# Filter rows with non-null os_name and os_vendor
dev_not_null = device_data.filter(col('os_name').isNotNull() & col('os_vendor').isNotNull())

# Count rows after filtering non-null OS data
dev_not_null.count()

# Get the most frequent os_name and os_vendor per brand
brand_os_count = dev_not_null.groupBy("brand_name", "os_name", "os_vendor").count()
window_spec = Window.partitionBy("brand_name").orderBy(desc("count"))
ranked_os = brand_os_count.withColumn("rank", row_number().over(window_spec))
most_frequent_os = ranked_os.filter(col("rank") == 1).select("brand_name", "os_name", "os_vendor")

# Show ranked OS data
ranked_os.show(truncate=False)

# Prepare mapping for most frequent OS per brand
most_frequent_os = most_frequent_os.withColumnRenamed("os_name", "most_frequent_os_name").withColumnRenamed("os_vendor", "most_frequent_os_vendor")

# Join device data with the most frequent OS mapping
device_with_os = device_data.join(most_frequent_os, on="brand_name", how="left")

# Fill missing os_name and os_vendor with the most frequent values
device_filled = device_with_os.withColumn(
    "os_name", when(col("os_name").isNull() & col("os_vendor").isNull(), col("most_frequent_os_name")).otherwise(col("os_name"))
).withColumn(
    "os_vendor", when(col("most_frequent_os_vendor").isNotNull() & col("os_vendor").isNull(), col("most_frequent_os_vendor")).otherwise(col("os_vendor"))
).drop("most_frequent_os_name", "most_frequent_os_vendor")

# Write cleaned CRM data to CSV
crm_final.coalesce(1).write.format("csv").option("header", "true").save("crm_cleaned.csv")

# Show final null counts for cleaned device data
device_filled.select([count(when(isnull(c), c)).alias(c) for c in device_filled.columns]).show(truncate=False)

# Write cleaned device data to CSV
device_filled.coalesce(1).write.format("csv").option("header", "true").save("device_cleaned.csv")



def upload_localfile_to_s3(local_file, s3_file):
    """
    Uploads a local file to an S3 bucket.

    Parameters:
    local_file (str): The path to the local file to be uploaded.
    s3_file (str): The name of the file in the S3 bucket.

    Returns:
    None

    Raises:
    FileNotFoundError: If the local file does not exist.
    Exception: If there is an error uploading the file to S3.
    """
    s3 = boto3.client(
            '3',
            aws_access_key_id=aws_options["awsAccessKeyId"],
            aws_secret_access_key=aws_options["awsSecretAccessKey"],
            region_name=aws_options["awsRegionName"]
        )
    bucket = aws_options["awsS3BucketName"]
    try:
        s3.upload_file(local_file, bucket, s3_file)
        logging.info(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
        print(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
    except FileNotFoundError:
        logging.error(f"The file was not found: {local_file}")
        print(f"The file was not found: {local_file}")
    except Exception as e:
        logging.error(f"Failed to upload {local_file} to {bucket}/{s3_file}: {e}", exc_info=True)
        print(f"Failed to upload {local_file} to {bucket}/{s3_file}: {e}")


def upload_from_localfolder_to_s3(local_folder):
    """
    This function uploads all CSV files from a local directory to an S3 bucket.
    Each file is renamed with the name of the parent directory.

    Parameters:
    local_folder (str): The path to the local directory containing the CSV files.

    Returns:
    None
    """
    # List all files in the directory
    for root, files in os.walk(local_folder):
        for file_name in files:
            # Check if the file is a CSV file
            if file_name.endswith('.csv'):
                # Construct the full path to the local file
                local_file = os.path.join(root, file_name)
                # Get the name of the parent directory
                folder_name = os.path.basename(local_folder)
                # Rename the file with the name of the parent directory
                s3_file = f"{folder_name}_{file_name}"
                # Upload the file to S3
                upload_localfile_to_s3(local_file, s3_file)

local_folders = ['./crm_cleaned.csv', './device_cleaned.csv']

# Upload the files from each directory
for folder in local_folders:
    upload_from_localfolder_to_s3(folder)
