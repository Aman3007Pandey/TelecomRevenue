"""Dag : Orchestrates the process from starting kafka to pushing data to snowlfake """
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

def run_bash_on_failure(context):
    """
    This function is called on task failure and writes context information
      to a file, then triggers a Bash command
    to run a Python script which sends the context information to a Slack channel
      or performs some other action.

    Parameters:
    context (dict): A dictionary containing context information about the DAG run,
      task instance, etc.
    """
    task_id = 'bash_on_failure_task'

    def write_context_info_to_file(context):
        """
        Writes task context information to a text file for later retrieval or processing.

        Parameters:
        context (dict): A dictionary containing context information about the DAG
          run, task instance, etc.
        """
        execution_date = context['execution_date']
        task_instance = context['task_instance']
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        run_id = context['dag_run'].run_id
        with open("/Users/amanpandey/Desktop/mock/src/utility/context_info.txt", "w") as file:
            file.write(f"Task Instance: {task_instance}\n")
            file.write(f"Execution Date: {execution_date}\n")
            file.write(f"Dag ID: {dag_id}\n")
            file.write(f"Task ID: {task_id}\n")
            file.write(f"Run ID: {run_id}\n")

    # Call the function to write the context info to a text file
    write_context_info_to_file(context)

    return BashOperator(
        task_id=task_id,
        bash_command='cd /Users/amanpandey/Desktop/mock && python3 -m src.utility.slack',
        dag=context['dag']
    ).execute(context=context)
"""### Section 1 DAG

    This DAG orchestrates a sequence of telecom-related tasks including starting
      a Kafka server, managing Kafka topics, 
    and running various scripts for data processing or analysis.
    **Default Arguments:**
    - `owner`: The owner of the DAG is `airflow`.
    - `depends_on_past`: The tasks do not depend on previous task runs.
    - `email_on_failure`: No email notifications will be sent on task failure.
    - `email_on_retry`: No email notifications will be sent on task retry.
    - `retries`: Tasks will be retried once on failure.
    - `retry_delay`: The retry will occur after a delay of 1 minute.
    - `on_failure_callback`: A function that is triggered on task failure
      to log context info and run a Slack notification.

    **Schedule and Execution:**
    - The DAG does not have a regular schedule and is intended to be run manually.
    - The `start_date` is set to May 28, 2024, and `catchup` is disabled to
      ensure it does not backfill for previous dates.
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback':run_bash_on_failure
}

# Define the DAG
with DAG(
    'section1_dag',
    default_args=default_args,
    description='A DAG to run telecom scripts in order',
    # schedule='*/10 * * * *',
    schedule=None,
    start_date=datetime(2024, 5, 28),
    catchup=False,
) as dag:

    kafka_server_start = BashOperator(
        task_id='start_kafka_server',
        bash_command='brew services start kafka & sleep 60',
    )

    kafka_topic = BashOperator(
        task_id='delete_kafka_topic_and_create',
        bash_command='kafka-topics --delete --topic rev_topic --bootstrap-server localhost:9092 \
            && kafka-topics --create --topic rev_topic --bootstrap-server localhost:9092 '
    )


    task_1 = BashOperator(
        task_id='run_script_1',
        bash_command='cd /Users/amanpandey/Desktop/mock && python3 -m src.section1.s3_to_mongo')
    task_2 = BashOperator(
        task_id='run_script_2',
        bash_command='cd /Users/amanpandey/Desktop/mock && python3 -m src.section1.producer')
    task_3 = BashOperator(
        task_id='run_script_3',
        bash_command='cd /Users/amanpandey/Desktop/mock && python3 -m src.section1.consumer')
    task_4 = BashOperator(
        task_id='run_script_4',
        bash_command='cd /Users/amanpandey/Desktop/mock && python3 \
           -m src.utility.clean_snowflake_data')
    kafka_server_stop = BashOperator(
        task_id='stop_kafka_server',
        bash_command='brew services stop kafka',
    )
    # Set task dependencies
    kafka_topic >> task_1 >> task_2 >> task_3 >> task_4 >> kafka_server_stop
