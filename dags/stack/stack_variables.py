from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': days_ago(1),
    'end_date': datetime(2022,10,30)
}

description = "A DAG exemplo de variables"

@dag(schedule=timedelta(days=2), default_args=default_args, catchup=False, tags=['stack', 'variables'], description = description)
def stack_variables():
    echo_volume_temp = BashOperator(task_id='echo_volume_temp',bash_command=("echo {{  var.value.volume_temp }}"))   

    echo_volume_temp
dag = stack_variables()