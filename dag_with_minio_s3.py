from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'himanshu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="dag_with_minio_s3_v1",
    default_args=default_args,
    description="This is my first DAG with s3 sensors",
    start_date=datetime(2024, 9, 6, 2),
    schedule_interval="@daily",
    ) as dag:
    task1 = S3KeySensor(
        task_id = "s3_key_sensor",
        bucket_name = "minio",
        bucket_key = "data.csv",
        aws_conn_id = "minio_conn", 
        
    )