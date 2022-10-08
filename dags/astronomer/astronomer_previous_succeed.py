from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import yaml

default_args = {
    'start_date': days_ago(1),
}

def _did_previous_dagrun_succeed(prev_execution_date_success,prev_excution_date):
    if (prev_execution_date_success is None):
        return 'process_data'
    if (prev_execution_date_success == prev_excution_date):
        return 'process_data'
    return 'stop'


@dag(schedule_interval='*/2 * * * *', default_args=default_args, catchup=False, tags=['astronomer', 'branch','previous'])
def astronomer_previous_succeed():
    did_previous_dagrun_succeed = BranchPythonOperator(task_id='did_previous_dagrun_succeed',python_callable=_did_previous_dagrun_succeed)
    process_data = EmptyOperator(task_id='process_data')
    stop = EmptyOperator(task_id='stop')

    did_previous_dagrun_succeed>>[process_data,stop]
dag = astronomer_previous_succeed()