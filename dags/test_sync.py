from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

@dag(
    schedule_interval='@daily', default_args=default_args, catchup=False,
    description="First DAG in airflow", tags=['stack', 'bash']
)
def test_sync():

    @task
    def echo_message(message):
        print(message)

    echo_message('Hello World!')
dag = test_sync()