from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': days_ago(1),
    'owner':'vale',
    'email':'viniciusdvale@gmail.com'

}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=['datacamp'])
def etl_dag():
    rand_number = BashOperator(task_id='rand_number',bash_command='echo $RANDOM')
    echo_ex = BashOperator(task_id='echo_ex',bash_command='echo "Exemple!"')
    rand_number
dag = etl_dag()