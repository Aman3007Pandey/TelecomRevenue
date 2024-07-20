"""  ### Section 2 DAG

    This DAG is designed to orchestrate the execution of telecom
      revenue analysis scripts using `dbt`.

    **Default Arguments:**
    - `owner`: The owner of the DAG is `airflow`.
    - `depends_on_past`: The tasks do not depend on previous task runs.
    - `email_on_failure`: No email notifications will be sent on task failure.
    - `email_on_retry`: No email notifications will be sent on task retry.
    - `retries`: Tasks will be retried once on failure.
    - `retry_delay`: The retry will occur after a delay of 1 minute.

    **Schedule and Execution:**
    - The DAG does not have a regular schedule and is intended to be run manually.
    - The `start_date` is set to June 6, 2024, and `catchup` is disabled to ensure it 
    does not backfill for previous dates."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'section2_dag',
    default_args=default_args,
    description='A DAG to run telecom scripts in order',
    schedule='0 0 * * 0',
    start_date=datetime(2024, 6, 6),
    catchup=False,
) as dag:
    dbt_testing = BashOperator(
        task_id='testing',
        bash_command='cd /Users/amanpandey/Desktop/mock/src/section2 \
        && dbt run --vars \'{"unit_testing": true}\'')
    dbt_transformation = BashOperator(
        task_id='transformation',
        bash_command='cd /Users/amanpandey/Desktop/mock/src/section2 \
          && dbt run --models silver')

    dbt_analysis = BashOperator(
        task_id='analysis',
        bash_command='cd /Users/amanpandey/Desktop/mock/src/section2 \
          && dbt run --models gold')
    # Set task dependencies
    dbt_testing >> dbt_transformation >> dbt_analysis
