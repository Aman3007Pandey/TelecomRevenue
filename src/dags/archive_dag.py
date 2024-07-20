"""
This Airflow DAG named `archive_dag` is designed to automate the process of
 fetching old data from a Snowflake database
and pushing it to Amazon S3 Glacier for archival purposes.
The DAG executes a Python script that handles this data migration,
ensuring that data older than a specific period (e.g., 10 weeks)
is moved to S3 Glacier for long-term storage and compliance.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'archive_dag',
    default_args=default_args,
    description='A DAG to run to transfer old data from snowflake to S3-Glacier',
    # schedule='0 0 1 * *',
    schedule='@yearly',
    start_date=datetime(2024, 5, 28),
    catchup=False,
) as dag:

    task_1 = BashOperator(
        task_id='run_archive_script',
        bash_command='cd /Users/amanpandey/Desktop/mock && python3 -m src.utility.archive')
    task_1
