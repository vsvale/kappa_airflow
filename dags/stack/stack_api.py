from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': days_ago(1),
    'end_date': datetime(2022,10,30)
}

description = "A DAG para consultar tickets da API polygon"

@dag(schedule='@daily', default_args=default_args, catchup=False, tags=['stack', 'variables','api'], description = description)
def stack_api():

    @task
    import requests
    import pandas as pd

    api_key = {{  var.value.api_key }}
    url = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/{}/{}?apiKey={}'.format({{ yesterday_ds }},{{ ds }},api_key)
    response = requests.get(url)
    data = response.json()

    if data['status']=='OK':
        df = pd.DataFrame(data['results'])

dag = stack_api()