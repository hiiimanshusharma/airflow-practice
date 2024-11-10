from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'himanshu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="first_dag_v4",
    default_args = default_args,
    description = "This is my first DAG",
    start_date = datetime(2024, 9, 6, 2),
    schedule_interval = "@daily",
) as  dag:
    task1 = BashOperator(
        task_id = "task1",
        bash_command = "echo 'Hello World'",
    )

    task2 = BashOperator(
        task_id = "task2",
        bash_command = "echo 'Hey I am second task, I am running after first task1'",
    )

    task3 = BashOperator(
        task_id = "task3",
        bash_command = "echo 'Hey I am third task, I am running after first task1 as same time as first task2'",
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    task1 >> [task2, task3]