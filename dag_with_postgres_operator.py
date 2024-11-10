from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'schedule_interval': '@daily',
}

with DAG(
    dag_id = "dag_with_postgres_operator_v2",
    default_args = default_args,
    description = "DAG with Postgres Operator",
    start_date = datetime(2024, 9, 5, 2),
) as dag:
    
    task1 = PostgresOperator(
        task_id = "task1",
        postgres_conn_id = "postgres_localhost",
        sql = """
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """,
    )

    task2 = PostgresOperator(
        task_id = "task2",
        postgres_conn_id = "postgres_localhost",
        sql = """
            INSERT INTO dag_runs (dt, dag_id) VALUES ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    task1 >> task2
