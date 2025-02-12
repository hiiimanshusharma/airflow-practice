from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'himanshu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hi {first_name} {last_name}, you are {age} years old")

def get_name(ti):
    ti.xcom_push(key="first_name", value="Himanshu")
    ti.xcom_push(key="last_name", value="Sharma")

def get_age(ti):
    ti.xcom_push(key="age", value=21)

with DAG(
    dag_id="python_opr_dag_v6",
    default_args=default_args,
    description="This is my first DAG with PythonOperators",
    start_date=datetime(2024, 9, 6, 2),
    schedule_interval="@daily",
    ) as dag:
    task1 = PythonOperator(
        task_id = "greet",
        python_callable = greet,
    )


    task2 = PythonOperator(
        task_id = "get_name",
        python_callable = get_name,
    )

    task3 = PythonOperator(
        task_id = "get_age",
        python_callable = get_age,
    )

    [task2, task3] >> task1


