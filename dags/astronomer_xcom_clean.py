from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1)
}

@dag(schedule='@daily', default_args=default_args, catchup=False, tags=['astronomer'])
def astronomer_xcom_clean():
    @task
    def extract():
        return 'my_data'

    @task
    def process(data):
        print(data)

    process(extract())
dag = astronomer_xcom_clean()