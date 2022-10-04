from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'start_date': days_ago(1),
    'owner':'vale',
    'email':'viniciusdvale@gmail.com'

}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=['datacamp'])
def etl_dag():
    rand_number = BashOperator(task_id='rand_number',bash_command='echo $RANDOM')
    echo_ex = BashOperator(task_id='echo_ex',bash_command='echo "Exemple!"')

    @task
    def printme():
        print("This goes in the logs!")

    @task
    def sleep(lenght_time):
        time.sleep(lenght_time)


    rand_number >> echo_ex
    rand_number >> printme()
    [echo_ex,printme()] >> sleep
dag = etl_dag()