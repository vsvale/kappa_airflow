from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'start_date': days_ago(1),
    'end_date': datetime(2022,10,30)
}

description = "A DAG exemplo de retryes"

@dag(schedule=timedelta(days=2), default_args=default_args, catchup=False, tags=['stack', 'retry'], description = description)
def stack_retry():
    @task(retries=10, retry_delay=timedelta(seconds=5))
    def random_number():
        import random
        n = random.random()
        print("Numero aleatorio: {}".format(n))
        if n > 0.3:
            raise Exception("Erro aqui!")
        print("coleta OK!")
    
    @task
    def process():
        print("Processing")
    

    random_number() >> process()
dag = stack_retry()