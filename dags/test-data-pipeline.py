from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "owner":"vinicius vale",
    "depends_on_past": False,
    "email": ["viniciusdvale@gmail.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'test_data_pipeline',
    default_args=default_args,
    start_date=datetime(2022,9,22),
    schedule_interval='@weekly',
    tags=['dev','bash']
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag
)
t1 >> [t2]