# imports
from datetime import datetime, timedelta
from airflow.decorators import dag,task_group
from airflow.utils.dates import days_ago
from os import getenv
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.sensors.external_task import ExternalTaskSensor

LAKEHOUSE = getenv("LAKEHOUSE", "lakehouse")

default_args = {
    'owner': 'vinicius da silva vale',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email': ['viniciusdvale@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_run': 1,
    'depends_on_past':False}

description = "DAG to create dim and facts and save in silver"

def example_silver():
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze',
        external_dag_id='example_bronze',
        external_task_id='t_list_bronze_example_salesorderdetail_folder',
        start_date=days_ago(1),
        execution_delta=timedelta(hours=1),
        timeout=3600,
        )
    
    @task_group()
    def dimcustomer_silver():
        # verify if new data has arrived on bronze bucket
        verify_customer_bronze = S3KeySensor(
        task_id='t_verify_customer_bronze',
        bucket_name=LAKEHOUSE,
        bucket_key='bronze/example/customer/*/*.parquet',
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120,
        aws_conn_id='minio')

        verify_customeraddress_bronze = S3KeySensor(
        task_id='t_verify_customeraddress_bronze',
        bucket_name=LAKEHOUSE,
        bucket_key='bronze/example/customeraddress/*/*.parquet',
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120,
        aws_conn_id='minio')

        verify_address_bronze = S3KeySensor(
        task_id='t_verify_address_bronze',
        bucket_name=LAKEHOUSE,
        bucket_key='bronze/example/address/*/*.parquet',
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120,
        aws_conn_id='minio')

        [verify_customer_bronze,verify_customeraddress_bronze,verify_address_bronze]
    wait_for_bronze >> dimcustomer_silver()
dag = example_silver()