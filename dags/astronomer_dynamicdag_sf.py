from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

tables = {
    'products': {
        'schedule_interval':'@weekly',
        'columns':['product','price','location'],
        'process_table':'maufactured_products',
        'store_table':'avaiable_products'
    },
    'customers': {
        'schedule_interval':'@daily',
        'columns':['customer','email','address'],
        'process_table':'stat_customers',
        'store_table':'raw_customers'
    },
    'products': {
        'schedule_interval':'@monthly',
        'columns':['loc','x','y'],
        'process_table':'geloc_customers',
        'store_table':'address_customers'
    },
}

def generate_dag(dag_id, start_date, schedule_interval, details):
    with DAG(dag_id,start_date=start_date,schedule_interval=schedule_interval) as dag:

        @task
        def extract():
            print(f'Extract columns {details["columns"]}')

        @task
        def process():
            print(f'Process table {details["process_table"]}')

        @task
        def store():
            print(f'Store table {details["store_table"]}')

        extract() >> process() >> store()
    return dag

for table, details in tables.items():
    dag_id = f'dag_{table}'
    globals()[dag_id] = generate_dag(dag_id,datetime(2022,10,5), details['schedule_interval'],details)