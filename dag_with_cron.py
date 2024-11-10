from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'himanshu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = "dag_with_cron_v1",
    default_args = default_args,
    description = "DAG with Cathup and Backfill",
    start_date = datetime(2024, 9, 5, 2),
    schedule_interval = "0  0 * * *",
    catchup = False,
) as dag:
    task1 = BashOperator(
        task_id = "task1",
        bash_command = "echo 'Hello World'",
    )

    task1