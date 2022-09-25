from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1)
}

with DAG('taskgroups_astronomer_demo', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    extracting = DummyOperator(task_id='extracting')

    with TaskGroup('processing_task_group') as processing_group:
        for l in ['A', 'B', 'C']:
            BashOperator(task_id=f'processing_{l}', bash_command='ls')

    loading = DummyOperator(task_id='loading')

    extracting >> processing_group >> loading