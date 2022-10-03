from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import yaml

default_args = {
    'start_date': days_ago(1),
}

def _is_holiday(ds):
    with open('holidays.yaml','r') as f:
        doc = yaml.load(f,Loader=yaml.SafeLoader)
        if (ds in doc['holidays']):
            return 'dont_process'
        return 'start'


@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=['astronomer', 'branch','calendar'])
def astronomer_calendar_branch():
    is_holiday = BranchPythonOperator(task_id='is_holiday',python_callable=_is_holiday)
    start = EmptyOperator(task_id='start')
    dont_process = EmptyOperator(task_id='dont_process')

    is_holiday>>[start,dont_process]
dag = astronomer_calendar_branch()