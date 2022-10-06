from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args ={
    'owner':'astronomer',
    'start_date':datetime(2022,10,5)
}

dag = DAG(
    dag_id,
    schedule_interval=scheduletoreplace,
    default_args=default_args,
    catchup=False
)

with dag:
    t1 = PostgresOperator(
        task_id='postgres_query',
        postgres_conn_id=connection_id,
        sql=querytoreplace
    )