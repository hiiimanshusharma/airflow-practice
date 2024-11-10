from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner': 'himanshu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="dag_taskflow_api_v2",
    default_args=default_args,
    description="DAG with TaskFlow API",
    start_date=datetime(2024, 9, 6, 2),
    schedule_interval="@daily")
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Himanshu",
            "last_name": "Sharma",
        }
    
    @task()
    def get_age():
        return 21
    
    @task()
    def greet(first_name, last_name, age):
        print(f"Hi {first_name} {last_name}, you are {age} years old")

    name_dict = get_name()
    age = get_age()
    greet(name_dict['first_name'], name_dict['last_name'], age)

him_dag = hello_world_etl()

