from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'start_date': days_ago(1),
}

description = "DAG with content of certitication"

@dag(
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=['astronomer','certification'],
    description=description,
    dagrun_timeout=timedelta(minutes=6))
def astronomer_cert_preparation():

    def clean():
        print('clean envoriment')

    @task
    def ml():
        clean()
        print("process with ml a")
    
    extract = BashOperator(task_id='extract', bash_command='echo "this commands extract my data"')
    store = EmptyOperator(task_id='store')

    extract >> ml() >> store
dag = astronomer_cert_preparation()