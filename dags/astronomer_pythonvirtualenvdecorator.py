from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False,tags=['astronomer', 'pyvenv'])
def astronomer_pythonvirtualenvdecorator():

    @task.virtualenv(
        use_dill=True,
        system_site_packages=False,
        requirements=[
            'dill==0.3.4'
            ],
    )
    def extract():
        import json # You must make your imports within the function
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        return order_data_dict
    extract()
dag = astronomer_pythonvirtualenvdecorator()