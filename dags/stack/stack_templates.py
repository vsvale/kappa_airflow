from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': days_ago(1),
    'end_date': datetime(2022,10,30)
}

description = "A DAG exemplo de template"

@dag(schedule='@daily', default_args=default_args, catchup=False, tags=['stack', 'template'], description = description)
def stack_templates():
    dt_exec = BashOperator(task_id='dt_exec', bash_command="echo data de execucao: {{ execution_date }}")
    dt_strf = BashOperator(task_id='dt_strf', bash_command="echo data de execucao formatada: {{ execution_date.strftime('%d/%m/%Y') }}")
    script = BashOperator(task_id='script', bash_command="/scripts/test.sh", env={'execution_date':'{{ ds }}'})

    dt_exec>>dt_strf>>script
dag = stack_templates()