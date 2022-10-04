from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from datetime import timedelta 
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['datacamp'])
def datacamp_simple_dag():
    rand_number = BashOperator(task_id='rand_number',bash_command='echo $RANDOM')

    rand_number
dag = datacamp_simple_dag()