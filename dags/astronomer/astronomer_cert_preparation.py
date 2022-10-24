from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain, cross_dowstream


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
    @task()
    def download_data():
        with open ('/tmp/my_file.txt','w') as f:
            f.write('my_data')

    wait_data = FileSensor(task_id='wait_data', fs_conn_id='fs_default', filepath='my_file.txt',poke_interval=30)

    cross_dowstream([download_data()],[wait_data])
dag = astronomer_cert_preparation()