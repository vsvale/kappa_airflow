from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from symbol import parameters

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

    clean_xcoms = PostgresOperator(task_id='clean_xcom', postgres_conn_id='postgres',sql='sql/delete_xcom.sql', parameters = {'dag_id':'astronomer_xcom_clean'})

    #create postgres connection on airflow, use ClusterIP as host
    process(extract()) >> clean_xcoms
dag = astronomer_xcom_clean()