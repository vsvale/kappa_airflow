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

LANDING_ZONE = getenv("LANDING_ZONE", "landing")
LAKEHOUSE = getenv("LAKEHOUSE", "lakehouse")


default_args = {
    'owner': 'vinicius da silva vale',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email': ['viniciusdvale@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

description = "DAG in charge of training ml models"

@dag(schedule='@daily', default_args=default_args,catchup=False,
tags=['example','spark','customer','s3','sensor','k8s'],description=description)

def example_bronze():
    @task_group()
    def example_customer_bronze():
        # verify if new data has arrived on landing bucket
        verify_customer_landing = S3KeySensor(
        task_id='t_verify_customer_landing',
        bucket_name=LANDING_ZONE,
        bucket_key='example/src-example-customer/*/*.parquet',
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120,
        aws_conn_id='minio')

        # use spark-on-k8s to operate against the data
        bronze_customer_spark_operator = SparkKubernetesOperator(
        task_id='t_bronze_customer_spark_operator',
        namespace='processing',
        application_file='example-customer-bronze.yaml',
        kubernetes_conn_id='kubeconnect',
        do_xcom_push=True)

        # monitor spark application using sensor to determine the outcome of the task
        monitor_bronze_customer_spark_operator = SparkKubernetesSensor(
        task_id='t_monitor_bronze_customer_spark_operator',
        namespace="processing",
        application_name="{{ task_instance.xcom_pull(task_ids='example_customer_bronze.t_bronze_customer_spark_operator')['metadata']['name'] }}",
        kubernetes_conn_id="kubeconnect")

        # Confirm files are created
        list_bronze_example_customer_folder = S3ListOperator(
        task_id='t_list_bronze_example_customer_folder',
        bucket=LAKEHOUSE,
        prefix='bronze/example/customer',
        delimiter='/',
        aws_conn_id='minio',
        do_xcom_push=True)    

        verify_customer_landing >> bronze_customer_spark_operator >> monitor_bronze_customer_spark_operator >> list_bronze_example_customer_folder
    example_customer_bronze()
dag = example_bronze()