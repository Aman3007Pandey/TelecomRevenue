""" Dag for docker orchestration """
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
    'docker_dag',
    default_args=default_args,
    description='A DAG to run telecom scripts in order',
    schedule=None,
    start_date=datetime(2024, 5, 28),
    catchup=False,
) as dag:

    kafka_mongo_server = BashOperator(
        task_id='start_kafka_mongo_server',
        bash_command='cd ~/Telecom-revenue-analysis &&  docker compose up -d\
              > ~/Telecom-revenue-analysis/logs/kafka_mongo_server_log.log 2>&1 & sleep 60',

    )

    kafka_topic = BashOperator(
        task_id='delete_kafka_topic_and_create',
        bash_command='cd ~/Telecom-revenue-analysis \
            &&  docker exec -it kafka /bin/bash \
                && kafka-topics.sh --delete --topic rev_topic --bootstrap-server localhost:9092 \
                && kafka-topics.sh --create --topic rev_topic --bootstrap-server localhost:9092 \
                > ~/Telecom-revenue-analysis/logs/kafka_topic_log.log 2>&1 & sleep 60'
    )

    kafka_mongo_server >> kafka_topic
    