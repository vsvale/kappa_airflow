from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=['astronomer', 'docker'])
def dockeroperator():

    @task.docker(image="python:latest",network_mode="bridge",api_version="auto")
    def randnumber():
        import random
        return [random.random() for _ in range(100)]

    randnumber()
dag = dockeroperator()