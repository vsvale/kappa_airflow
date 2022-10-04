from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from datetime import timedelta 
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': days_ago(1),
    'owner':'vale',
    'email':'viniciusdvale@gmail.com',
    'email_on_failure':False,
    'email_on_retry:': False,
    'email_on_success':False,
    'sla': timedelta(minutes=10)

}


@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['datacamp'])
def datacamp_simple_dag():
    rand_number = BashOperator(task_id='rand_number',bash_command='echo $RANDOM')

    rand_number
dag = datacamp_simple_dag()