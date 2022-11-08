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

@dag(schedule='@daily', default_args=default_args,catchup=False,
tags=['example','spark','customer','s3','sensor','k8s'],description=description)

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
    application_name="{{ task_instance.xcom_pull(task_ids='t_bronze_customer_spark_operator')['metadata']['name'] }}",
    kubernetes_conn_id="kubeconnect")

    # use spark-on-k8s to operate against the data
    bronze_customer_spark_operator_verify = SparkKubernetesOperator(
    task_id='t_bronze_customer_spark_operator_verify',
    namespace='processing',
    application_file='example-customer-bronze-verify.yaml',
    kubernetes_conn_id='kubeconnect',
    do_xcom_push=True)

    # monitor spark application using sensor to determine the outcome of the task
    monitor_bronze_customer_spark_operator_verify = SparkKubernetesSensor(
    task_id='t_monitor_bronze_customer_spark_operator_verify',
    namespace="processing",
    application_name="{{ task_instance.xcom_pull(task_ids='t_bronze_customer_spark_operator_verify')['metadata']['name'] }}",
    kubernetes_conn_id="kubeconnect")

    verify_customer_landing >> bronze_customer_spark_operator >> monitor_bronze_customer_spark_operator >> bronze_customer_spark_operator_verify
    bronze_customer_spark_operator_verify >> monitor_bronze_customer_spark_operator_verify
dag = example_customer_bronze()