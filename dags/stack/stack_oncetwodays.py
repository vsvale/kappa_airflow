from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': days_ago(1),
    'end_date': datetime(2022,10,30)
}
@dag(schedule=timedelta(days=2), default_args=default_args, catchup=False, tags=['stack', 'daily'])
def stack_cron():
    print_date = BashOperator(task_id='print_date',bash_command=("echo Iniciando a tarefa: &&" "date"))
    processing_data = BashOperator(task_id='processing_data',bash_command=("echo Processando os dados... &&" "sleep 5"))

    print_date >> processing_data
dag = stack_cron()