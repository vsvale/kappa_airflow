from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
import yaml

default_args = {
    'start_date': days_ago(1),
}


@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=['astronomer', 'branch'])
def astronomer_calendar_branch():
    
    @task.branch()
    def is_holliday(ds):
        with open('include/holidays.yaml','r') as f:
            doc = yaml.load(f,Loader=yaml.SafeLoader)
            if (ds in doc['holidays']):
                return 'dont_process'
            return 'start'

    start = EmptyOperator(task_id='start')
    dont_process = EmptyOperator(task_id='dont_process')

    is_holliday()>>[start,dont_process]
dag = astronomer_calendar_branch()