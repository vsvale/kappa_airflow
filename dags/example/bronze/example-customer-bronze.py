# imports
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from os import getenv
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

LANDING_ZONE = getenv("LANDING_ZONE", "landing")

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

@dag(schedule_interval='@daily', default_args=default_args,catchup=False,
tags=['example','spark','customer','s3','sensor','k8s'],description=description)

def example_customer_bronze():
    # verify if new data has arrived on landing bucket
    verify_customer_landing = S3KeySensor(
    task_id='t_verify_customer_landing',
    bucket_name=LANDING_ZONE,
    bucket_key='example/src-example-customer/2022/11/07/15/*.parquet',
    wildcard_match=True,
    timeout=18 * 60 * 60,
    poke_interval=120,
    aws_conn_id='minio')

    verify_customer_landing
dag = example_customer_bronze()