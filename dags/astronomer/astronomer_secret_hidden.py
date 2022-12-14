from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=['astronomer', 'secret'])
def astronomer_secret_hidden():

    @task
    def my_very_unsecure_task():
        print(Variable.get("secret"))

    my_very_unsecure_task()
dag = astronomer_secret_hidden()