
Telecom Customer Revenue Analysis
===

### Description 
This project involves data analytics for telecom companies using data provided by them. The primary goal is to simulate data ingestion, build ETL pipelines, and derive insights from the data. The analysis spans customer demographics, device attributes, and revenue patterns over time.


## Authors

- [Vansh Tandon(TAS201)](https://www.github.com/tandonvansh)
- [Aman Pandey (TAS208)](https://github.com/AmanSigmoidAnalytics)
- [Vranda Mahajan (TAS202)](https://github.com/vvrandaa)
- [Mohan Reddy (TAS212)](https://github.com/mohan-333)

## Table of Contents

- [Project Structure](#project-structure)
  - [Config Directory](#config-directory)
  - [Src Directory](#src-directory)
- [Dags Directory](#dags-directory)
- [Section1](#section1)
  - [End to End pipeline architecture](#end-to-end-pipeline-architecture)
  - [Data Cleaning `data_cleaning.py` Overview](#data-cleaning-datacleaningpy-overview)
  - [Dependencies](#dependencies)
  - [Configuration](#configuration)
  - [Logging](#logging)
  - [Spark Session Creation](#spark-session-creation)
  - [Reading Data from S3](#reading-data-from-s3)
  - [Data Cleaning Steps](#data-cleaning-steps)
    - [Repetition Counts](#repetition-counts)
    - [Fuzzy Matching for Gender](#fuzzy-matching-for-gender)
    - [Filter Unreasonable `year_of_birth` Values](#filter-unreasonable-yearofbirth-values)
    - [Filter and Prioritize Records](#filter-and-prioritize-records)
  - [Saving Cleaned Data](#saving-cleaned-data)
  - [Uploading Data to S3](#uploading-data-to-s3)
    - [Upload Single File](#upload-single-file)
    - [Upload Directory](#upload-directory)
  - [Running the Script](#running-the-script)
  - [Data Cleaning Jupyter Notebook `data_cleaning.ipynb` Overview](#data-cleaning-jupyter-notebook-datacleaningipynb-overview)
    - [Steps](#steps)
  - [Running the Notebook](#running-the-notebook)
  - [Mongo Cleaning `mongo_cleaning.py` Overview](#mongo-cleaning-mongocleaningpy-overview)
    - [Prerequisites](#prerequisites)
    - [Steps](#steps-1)
    - [Running the Script](#running-the-script-1)
  - [Mongo Insertion `s3_to_mongo.py` Overview](#mongo-insertion-s3tomongopy-overview)
    - [Functionality](#functionality)
    - [Dependencies](#dependencies-1)
    - [Usage](#usage)
    - [Note](#note)
  - [Kafka Producer `producer.py` Overview](#kafka-producer-producerpy-overview)
    - [Functionality](#functionality-1)
    - [Dependencies](#dependencies-2)
    - [Usage](#usage-1)
    - [Note](#note-1)
  - [Unit Tests](#unit-tests)
    - [Test `test_aws_connection.py` Overview](#test-testawsconnectionpy-overview)
      - [AWS Connector Unit Tests](#aws-connector-unit-tests)
    - [Running the Tests](#running-the-tests)
    - [Mocking](#mocking)
    - [Test Snowflake Connector](#test-snowflake-connector)
    - [Similary for running other tests file use](#similary-for-running-other-tests-file-use)
  - [Consumer `consumer.py` Overview](#consumer-consumerpy-overview)
    - [Functionality](#functionality-2)
    - [Dependencies](#dependencies-3)
    - [Usage](#usage-2)
    - [Note](#note-2)
- [Section 2](#section-2)
  - [DBT Setup and Execution](#dbt-setup-and-execution)
  - [To Run Tests](#to-run-tests)
  - [To Run on Snowflake Data](#to-run-on-snowflake-data)
- [Setting up environment variables](#setting-up-environment-variables)
  - [snowflake.env](#snowflakeenv)
  - [mongodb.env](#mongodbenv)
  - [kafka.env](#kafkaenv)
  - [aws.env](#awsenvironment)
- [Section3](#section3)
  - [Device Analysis SQL](#device-analysis-sql)
    - [Overview](#overview)
    - [SQL Queries Explained](#sql-queries-explained)
      - [Query 1](#query-1)
      - [Query 2](#query-2)
      - [Query 3](#query-3)
      - [Query 4](#query-4)
      - [Query 5](#query-5)
    - [Analysis for 1NF Generation](#analysis-for-1nf-generation)
      - [Steps for 1NF Normalization](#steps-for-1nf-normalization)
    - [Conclusion](#conclusion)

## Project Structure
![image](https://hackmd.io/_uploads/SkmjOcNBR.png)

### Config Directory
The config directory contains configuration files required for connecting to various services used in the project.

`aws_config.py`: Configuration for AWS services.
`kafka_config.py`: Configuration for Kafka message broker.
`mongo_config.py`: Configuration for MongoDB.
`snowflake_config.py`: Configuration for Snowflake.

### Src Directory

The `src` directory contains all the source code organized into different sections and utility scripts.


* `dags/`: Contains DAG (Directed Acyclic Graph) files for Airflow.
* `section1/`: Scripts related to Section 1 of the project.
* `section2/`: Scripts related to Section 2 of the project.
* `section3/`: Scripts related to Section 3 of the project.
* `utility/`: Utility scripts used across different sections.


    

### Root Files


`.dockerignore`: Specifies which files and directories to ignore in Docker builds.
`.gitignore`: Specifies which files and directories to ignore in Git.
`docker-compose.yml`: Defines services, networks, and volumes for Docker.
`Dockerfile.mongo`: Dockerfile to build a custom MongoDB image.
`README.md`: Project documentation.
`requirements.txt`: List of Python dependencies.


## Dags Directory

* `archive_dag.py`: Archives old data from Snowflake to Amazon S3 Glacier.
* `docker_dag.py`: Runs Airflow on Docker.
* `section1_dag.py`: Manages Kafka and runs telecom-related scripts.
* `section2_dag.py`: Executes DBT transformations using Airflow.
* `slack_dag.py`: Sends Slack notifications on DAG success or failure.

## Section1


![image](https://hackmd.io/_uploads/H1hKFoVBR.png)


### End to End pipeline architecture

![image](https://hackmd.io/_uploads/By-w5iEBC.png)


### Data Cleaning `data_cleaning.py` Overview

This script is designed to clean and preprocess CRM (Customer Relationship Management) and device data stored in CSV files on AWS S3. It performs several data cleaning steps, including reading data from S3, handling missing values, applying fuzzy matching for gender values, and filtering rows based on specific conditions. The cleaned data is then saved locally and uploaded back to S3.


### Dependencies

The script requires the following Python libraries:
- `pyspark`: For Spark SQL and DataFrame operations.
- `logging`: For logging messages to a file.
- `os`: For directory and file operations.
- `boto3`: For interacting with AWS S3.
- `matplotlib.pyplot`: For plotting (if needed for future extensions).
- `jellyfish`: For fuzzy string matching.

### Configuration

Ensure you have a `config/aws_config.py` file with the necessary AWS options:
- `awsAccessKeyId`: Your AWS access key ID.
- `awsSecretAccessKey`: Your AWS secret access key.
- `awsRegionName`: Your AWS region name.
- `awsS3BucketName`: Your S3 bucket name.

### Logging

The script sets up logging to write messages to `logs/data_cleaning.log`. If the `logs` directory does not exist, it will be created automatically.

### Spark Session Creation

A Spark session is created with configurations for memory and S3 access.

### Reading Data from S3

The script reads CSV files from S3 into Spark DataFrames.

### Data Cleaning Steps

#### Repetition Counts

- Calculate the number of repetitions of `msisdn` values and their counts.
- Group by the count of repetitions and count the number of `msisdn` for each count.

#### Fuzzy Matching for Gender

- Define a function for fuzzy matching gender values using the Jaro-Winkler similarity metric.
- Apply fuzzy matching to clean the gender column.

#### Filter Unreasonable `year_of_birth` Values

- Filter out rows with `year_of_birth` values greater than or equal to 2024.

#### Filter and Prioritize Records

- Define window specifications for ordering records.
- Add columns to check for adjacent active/suspend pairs within the same SIM card.
- Create a flag to identify pairs to be filtered out.
- Filter out pairs based on the flag and prepare for final selection.
- Handle deactivation as the end of the life cycle.
- Count non-null values in columns.
- Prioritize records based on status and non-null count.
- Select records with the highest priority.

### Saving Cleaned Data

- Save the cleaned CRM data to a CSV file.
- Save the cleaned device data to a CSV file.

### Uploading Data to S3

#### Upload Single File

Define a function to upload a local file to S3.

#### Upload Directory

Define a function to upload all CSV files from a local directory to S3, renaming each file with the name of the parent directory.

#### Running the Script

1. Ensure your AWS credentials and S3 configuration are correctly set in `config/aws_config.py`.
2. Run the script `python3 -m src.section1.data_cleaning` to perform the data cleaning and preprocessing steps.
3. Check the logs in `logs/data_cleaning.log` for detailed information about the script's execution.
4. The cleaned data will be saved locally as `crm_cleaned.csv` and `device_cleaned.csv`.
5. The cleaned data will also be uploaded back to the specified S3 bucket.

### Data Cleaning Jupyter Notebook `data_cleaning.ipynb` Overview

This Jupyter Notebook performs the same data cleaning and preprocessing steps as outlined in `data_cleaning.py`. The main difference is that it does not include integration with AWS S3. The notebook is designed to demonstrate the step-by-step process of cleaning CRM (Customer Relationship Management) and device data.

#### Steps

1. **Logging**: Sets up logging to write messages to `logs/data_cleaning.log`.
2. **Spark Session Creation**: Creates a Spark session with configurations for memory.
3. **Reading Data**: Reads CSV files into Spark DataFrames.
4. **Repetition Counts**: Calculates the number of repetitions of `msisdn` values and their counts.
5. **Fuzzy Matching for Gender**: Cleans the gender column using fuzzy matching with the Jaro-Winkler similarity metric.
6. **Filter Unreasonable `year_of_birth` Values**: Filters out rows with `year_of_birth` values greater than or equal to 2024.
7. **Filter and Prioritize Records**:
    - Adds columns to check for adjacent active/suspend pairs within the same SIM card.
    - Filters out pairs based on the flag and prepares for final selection.
    - Handles deactivation as the end of the life cycle.
    - Counts non-null values in columns.
    - Prioritizes records based on status and non-null count.
    - Selects records with the highest priority.
8. **Saving Cleaned Data**: Saves the cleaned CRM data to a CSV file and the cleaned device data to a CSV file.

### Running the Notebook

1. Ensure your CSV files are correctly placed in the local directory.
2. Run the notebook cells sequentially to perform the data cleaning and preprocessing steps.
3. The cleaned data will be saved locally as `crm_cleaned.csv` and `device_cleaned.csv`.


### Mongo Cleaning `mongo_cleaning.py` Overview

This Python script performs data cleaning and preprocessing using MongoDB for CRM (Customer Relationship Management) and device data. The script includes various functions to clean brand names, OS producers, and gender data, count null values, and ensure unique `msisdn` values with prioritized records.

#### Prerequisites

- MongoDB server
- pymongo library
- jellyfish library

#### Steps

1. **Logging**: Not explicitly mentioned, but assume appropriate logging is set up within each function.
2. **MongoDB Connection**: Initialize a connection to the MongoDB database using the `MongoDBConnector` utility.
3. **Cleaning Brand Names**: The `clean_brand_names` function replaces 'Null' values in the `os_name` field with the most frequent `os_name` value for each unique `brand_name`.
4. **Cleaning OS Producers**: The `clean_os_producers` function replaces 'Null' values in the `os_vendor` field with the most frequent `os_vendor` value for each unique `os_name`.
5. **Cleaning Gender Values**: The `clean_gender` function cleans the gender values by converting them to lowercase, stripping leading/trailing spaces, and replacing 'Null' with 'other'. It uses the Jaro-Winkler distance algorithm to match predefined male and female terms.
6. **Counting Null Values**: The `count_null_values` function calculates the number of null or 'Null' values in a given document.
7. **Adding Null Count Field**: The `add_null_count_field` function iterates over all documents in the collection, calculates the number of null or 'Null' values, and updates the document with the calculated `null_count`.
8. **Ensuring Unique `msisdn` Values**: The `null_add_and_msisdn_unique` function:
    - Adds a new field `null_count` to each document.
    - Sorts documents by `msisdn`, `null_count`, `system_status`, and `gender`.
    - Assigns a rank to each document within each `msisdn` group.
    - Retains only the documents with rank 1.
    - Updates the collection with the processed documents.
9. **Testing Collection Conditions**: The `test_collection_conditions` function tests the conditions of the MongoDB collection, including checking for duplicate `msisdn` values, verifying the correct computation of `null_count`, and ensuring each document has the highest `system_status` and `gender` within its `msisdn` group.

#### Running the Script

1. Ensure MongoDB server is running and accessible.
2. Configure MongoDB connection settings in `mongo_config.py`.
3. Run the script:
    ```sh
    python3 -m src.section1.mongo_cleaning.py
    ```
4. The script will clean the data and update the MongoDB collections accordingly.


### Mongo Insertion `s3_to_mongo.py` Overview

This Python script `s3_to_mongo.py` reads CSV files from an S3 bucket, converts them to JSON format, and inserts the data into a MongoDB database. It includes functions to read CSV files from S3, convert them to JSON using Pandas, and insert the JSON data into MongoDB collections.

#### Functionality

1. **Reading CSV from S3**: The `read_csv_from_s3` function reads a CSV file from an S3 bucket using the boto3 library and returns it as a pandas DataFrame.

2. **Converting CSV to JSON**: The `convert_csv_to_json` function reads CRM and Device CSV files from S3, converts them to JSON format using Pandas, and returns JSON strings.

3. **Inserting Data into MongoDB**: The script loads the JSON data into Python dictionaries, establishes a connection to MongoDB using `MongoDBConnector`, and inserts the data into the specified MongoDB collections using the `insert_many` method.

#### Dependencies

- boto3
- pandas
- pymongo

#### Usage

1. Ensure that the AWS credentials and S3 bucket configuration are correctly set up in the `aws_config.py` file.
2. Configure MongoDB connection settings in `mongo_config.py`.
3. Run the script:
    ```sh
    python3 -m src.section1.s3_to_mongo
    ```
4. The script will read CSV files from the specified S3 bucket, convert them to JSON, and insert the data into MongoDB collections.

#### Note

This script assumes that the CSV files in the S3 bucket are cleaned and ready for ingestion. It performs minimal data quality checks such as reading from S3 and converting to JSON, but comprehensive data quality checks should ideally be performed before ingesting the data into MongoDB. This includes checking for duplicates, missing column values, and ensuring data integrity.


### Kafka Producer `producer.py` Overview

The `producer.py` script reads data from an S3 bucket, processes it, filters it based on a specified week number, converts it to JSON format, and pushes it to a Kafka topic using a Kafka producer.

#### Functionality

1. **Reading Data from S3**: The script fetches data from an S3 bucket using the `fetch_data_from_s3` method of the `AWSConnector` class.

2. **Processing Data**: It filters the data based on a specified week number and prepares it for ingestion into Kafka.

3. **Pushing Data to Kafka**: Using the `Producer` class from the `confluent_kafka` library, the script sends JSON-formatted messages to the specified Kafka topic.

#### Dependencies

- confluent_kafka
- boto3

#### Usage

1. Ensure that the AWS credentials and S3 bucket configuration are correctly set up in the `aws_config.py` file.
2. Configure Kafka broker details in the `kafka_config.py` file.
3. Run the script:
    ```sh
    python3 -m src.section1.producer
    ```
4. The script will connect to Kafka, process data for the current week number, and push it to the specified Kafka topic.

#### Note

- The script is designed to push data from S3 to Kafka for the current week number. Ensure that the week number logic aligns with the expected data partitioning in Kafka.
- Data processing and filtering are performed before pushing data to Kafka. Ensure that the data quality and filtering logic meet the requirements before ingestion.

### Unit Tests

#### Test `test_aws_connection.py` Overview

Run the file using this command
python -m pytest src/section1/unit_tests/test_aws_connection.py `
#### AWS Connector Unit Tests

This repository contains unit tests for the AWSConnector class, which is part of the utility module in your project. The AWSConnector class is responsible for connecting to Amazon S3, fetching data from S3, and pushing data to S3.

#### Running the Tests

To run the unit tests, execute the following command in your terminal:


This command will execute all the test cases defined in the `test_aws_connector.py` file.



#### Test Snowflake Connector

- **test_connect_success**: This test verifies that the `connect_to_s3` method of AWSConnector establishes a connection to Amazon S3 successfully. It uses mocking to simulate the connection object returned by the `connect_to_s3` method.
  
- **test_fetch_data_from_s3**: This test ensures that the `fetch_data_from_s3` method of AWSConnector retrieves data from the specified bucket and file key in Amazon S3. It mocks the connection to S3 and verifies that the method is called with the correct arguments.

- **test_push_data_to_s3**: This test validates that the `push_data_to_s3` method of AWSConnector uploads data to the specified bucket and folder path in Amazon S3. Similar to the previous tests, it mocks the connection to S3 and checks if the method is called with the expected arguments.

#### Mocking

Mocking is utilized in the unit tests to isolate the AWSConnector class from its dependencies, such as the connection to Amazon S3. This ensures that the tests focus solely on the behavior of the AWSConnector methods without relying on external services.


#### Similary for running other tests file use

`python -m pytest src/section1/unit_tests/<test_file_name>.py `



### Consumer `consumer.py` Overview

The `consumer.py` script is designed to integrate data from MongoDB, Kafka, and Snowflake using Apache Spark. It retrieves data from MongoDB collections, consumes streaming data from Kafka, performs join operations, and writes the final dataset to Snowflake.

#### Functionality

1. **Read Data from MongoDB**: The script reads data from two MongoDB collections: `crmCollectionName` and `deviceCollectionName`.

2. **Read Data from Kafka**: It consumes streaming data from a Kafka topic (`kafkaTopicName`).

3. **Data Processing and Joining**: 
   - The MongoDB data is loaded into Spark DataFrames.
   - The Kafka streaming data is processed, ensuring the necessary fields are present and data types are correct.
   - The script performs join operations between the Kafka data and the MongoDB data, creating a unified dataset.

4. **Write Data to Snowflake**: The final joined dataset is written to a specified Snowflake table (`sfTableName`). Each batch of data is written as a new batch to Snowflake.

5. **Idle Monitoring**: An `IdleMonitor` class is implemented to monitor the idle time of the Spark Streaming query. If no new data is received for a specified timeout, the query is stopped to prevent resource wastage.

#### Dependencies

- pyspark
- confluent_kafka
- pymongo
- snowflake-connector-python

#### Usage

1. Configure the MongoDB connection details, Kafka broker details, and Snowflake connection details in their respective configuration files (`mongo_config.py`, `kafka_config.py`, `snowflake_config.py`).
2. Run the script:
    ```sh
    python3 -m src.section1.consumer
    ```

#### Note

- Ensure that the configurations for MongoDB, Kafka, and Snowflake are correctly set up before running the script.
- The script assumes that the Kafka topic contains data in JSON format compatible with the specified schema.
- Idle monitoring ensures that the Spark Streaming query stops if no new data is received within a specified time period, preventing unnecessary resource consumption.


## Section 2

This directory contains the DBT setup and execution details to run the automation.

![image](https://hackmd.io/_uploads/HJZ8roNBC.png)

* **logs/dbt.log**: Logs generated by DBT for debugging and tracking purposes.
* **macros**: Contains SQL macros for reusable SQL code.
* **models**: Contains the DBT models divided into:
    * **gold**: Final analyzed views ready for consumption.
    * **silver**: Clean and joined tables for running analysis.
* **sources**: Source tables.
* **seeds**: Contains sample data files for testing DBT execution.
The seeds folder contains sample data files to test DBT execution.
Users need to set up sample data files in the seeds directory:
    * **input_crm.csv**
    * **input_device.csv**
    * **input_rev.csv**
* **target**: Contains the compiled SQL files and run results.

A sample `dbt_project.yml` should be set up by the user to run DBT. This configuration file defines the project settings, including model paths, test paths, and variables.

```yaml
name: 'my_dbt_project'
version: '1.0.0'
config-version: 2

# Define the model paths
model-paths: ["models"]
# Define the test paths
test-paths: ["tests"]
# Define the seed paths
seed-paths: ["seeds"]
# Define the macro paths
macro-paths: ["macros"]

profile: 'my_profile'

vars:
  unit_testing: false
  schema: silver_layer
  test_schema: test_silver_layer
  gold_schema: gold_layer
  test_gold_schema: test_gold_layer

models:
  my_dbt_project:
    silver:
      +materialized: table
    gold: 
      +materialized: view
seeds:
  my_dbt_project:  
      +schema: SEEDS  
```

A profiles.yml file should be set up containing the Snowflake connection details.

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: 'your_account'
      user: 'your_username'
      password: 'your_password'
      role: 'role'  
      warehouse: 'warehouse_name'
      database: 'db_name'
      schema: 'schema_name'
      authenticator: 'snowflake'  
      threads: 4
      client_session_keep_alive: False

```
**To Run Tests :**

`dbt run --vars '{"unit_testing": true}'`

**To Run on Snowflake data :**

`dbt run`


## Setting up environment variables

### snowflake.env

SF_URL=`your snowflake account url`

SF_USER=`your snowflake username`

SF_PASSWORD=`your snowflake account password`

SF_WAREHOUSE=`warehouse name`

SF_WAREHOUSE_SIZE=`warehouse size`

SF_DATABASE=`database name`

SF_SCHEMA=`schema name`

SF_TABLE_NAME=`staging table name`

CLEAN_SF_TABLE_NAME=`final clean table name`


### mongodb.env

MONGO_URI=`your mongo connection string`

MONGO_DB_NAME=`database name`

CRM_COLLECTION_NAME=`CRM collection name`

DEVICE_COLLECTION_NAME=`DEVICE collection name`

SMALL_CRM_COLLECTION_NAME=`SMALL CRM collection name`

SMALL_DEVICE_COLLECTION_NAME=`SMALL DEVICE collection name`

CLEAN_CRM_COLLECTION_NAME=`CLEAN CRM collection name`

CLEAN_DEVICE_COLLECTION_NAME=`CLEAN DEVICE collection name`

### kafka.env

KAFKA_BOOTSTRAP_SERVERS=`your bootstrap server like localhost:9092`

KAFKA_GROUP_ID=`your group id`

KAFKA_AUTO_OFFSET_RESET=`earliest`

KAFKA_TOPIC_NAME=`your topic name`

KAFKA_FILE_NAME=`revenue file name`

### aws.env

AWS_ACCESS_KEY_ID=`your aws access key`

AWS_SECRET_ACCESS_KEY=`your aws secret access key`

AWS_REGION_NAME=`your region name`

AWS_S3_BUCKET_NAME=`your bucket name`

AWS_S3_ARCHIVE_FOLDER=`your s3 archive folder name`


## Section3
### Device Analysis SQL

#### Overview

The `device_analysis.sql` script is designed to identify and analyze entries in the `device` table where the `MODEL_NAME` column contains multiple values. This is a crucial step in ensuring data normalization, specifically achieving the First Normal Form (1NF), which requires that each column in a table contains atomic (indivisible) values.

#### SQL Queries Explained

##### Query 1
This query retrieves a record for a specific `msisdn` where the `MODEL_NAME` column contains multiple values separated by commas. This indicates a violation of 1NF as it combines several model names into a single field.

##### Query 2
This query fetches another record for a different `msisdn` that also has multiple values in the `MODEL_NAME` column. This example further illustrates the need for normalization to separate these values into distinct records.

##### Query 3
This query retrieves a record where the `MODEL_NAME` column again contains multiple values, emphasizing the recurring pattern of data that violates 1NF. Each of these values needs to be split into separate rows.

##### Query 4
This query fetches a record where multiple model names are encapsulated within parentheses in the `MODEL_NAME` column. This format also indicates multiple values within a single field that should be separated.

##### Query 5
This query retrieves a record where the `MODEL_NAME` column contains multiple values separated by commas, including some blank values. This further highlights the inconsistency and the need for atomic values in each column.

### Analysis for 1NF Generation

The queries in this script identify rows in the `device` table that do not conform to 1NF. 1NF requires each column to contain only atomic values.

#### Steps for 1NF Normalization
1. **Identify Multi-Valued Attributes**: The queries identify rows where the `MODEL_NAME` column contains multiple values separated by commas or parentheses.
2. **Split Multi-Valued Attributes**: These identified multi-valued attributes need to be split into separate rows. For instance, a record with `MODEL_NAME` as "P500,P400,,Z1,Z2" should be split into multiple records, each with one of these model names.
3. **Ensure Atomic Values**: After splitting, ensure that each record in the `device` table has atomic values for all columns.

### Conclusion

The `device_analysis.sql` script is a part of the data normalization process to achieve 1NF by identifying and correcting multi-valued attributes in the `MODEL_NAME` column. This ensures that the `device` table maintains atomic values for each column, thereby improving data quality and consistency.



