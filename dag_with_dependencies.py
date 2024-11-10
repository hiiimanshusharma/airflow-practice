from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'himanshu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def get_sklearn():
    import sklearn
    print(f"sklearn version: {sklearn.__version__}")


with DAG(
    dag_id="dag_with_dependencies_v1",
    default_args=default_args,
    description="This is my first DAG with PythonOperators",
    start_date=datetime(2024, 9, 6, 2),
    schedule_interval="@daily",
    ) as dag:
    task1 = PythonOperator(
        task_id = "sklearn",
        python_callable = get_sklearn,
    )

    task1

