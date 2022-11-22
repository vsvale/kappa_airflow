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

description = "DAG to create dim and facts and save in gold"

@dag(schedule='@daily', default_args=default_args,catchup=False,
tags=['example','spark','gold','s3','sensor','k8s','kafka'],description=description)
def example_gold():
    @task_group()
    def dimcurrency_gold():
        # use spark-on-k8s to operate against the data
        gold_dimcurrency_spark_operator = SparkKubernetesOperator(
        task_id='t_gold_dimcurrency_spark_operator',
        namespace='processing',
        application_file='example-dimcurrency-gold.yaml',
        kubernetes_conn_id='kubeconnect',
        do_xcom_push=True)

        # monitor spark application using sensor to determine the outcome of the task
        monitor_gold_dimcurrency_spark_operator = SparkKubernetesSensor(
        task_id='t_monitor_gold_dimcurrency_spark_operator',
        namespace="processing",
        application_name="{{ task_instance.xcom_pull(task_ids='dimcurrency_gold.t_gold_dimcurrency_spark_operator')['metadata']['name'] }}",
        kubernetes_conn_id="kubeconnect")

        # Confirm files are created
        list_gold_example_dimcurrency_folder = S3ListOperator(
        task_id='t_list_gold_example_dimcurrency_folder',
        bucket=LAKEHOUSE,
        prefix='gold/example/dimcurrency',
        delimiter='/',
        aws_conn_id='minio',
        do_xcom_push=True)

        # verify if new data has arrived on gold bucket
        verify_dimcurrency_gold = S3KeySensor(
        task_id='t_verify_dimcurrency_gold',
        bucket_name=LAKEHOUSE,
        bucket_key='gold/example/dimcurrency/*.parquet',
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120,
        aws_conn_id='minio')

        # use spark-on-k8s to operate against the data
        ysql_dimcurrency_spark_operator = SparkKubernetesOperator(
        task_id='t_ysql_dimcurrency_spark_operator',
        namespace='processing',
        application_file='example-dimcurrency-ysql.yaml',
        kubernetes_conn_id='kubeconnect',
        do_xcom_push=True)

        # monitor spark application using sensor to determine the outcome of the task
        monitor_ysql_dimcurrency_spark_operator = SparkKubernetesSensor(
        task_id='t_monitor_ysql_dimcurrency_spark_operator',
        namespace="processing",
        application_name="{{ task_instance.xcom_pull(task_ids='dimcurrency_gold.t_gold_dimcurrency_spark_operator')['metadata']['name'] }}",
        kubernetes_conn_id="kubeconnect")



        gold_dimcurrency_spark_operator >> monitor_gold_dimcurrency_spark_operator >> list_gold_example_dimcurrency_folder >> verify_dimcurrency_gold >> ysql_dimcurrency_spark_operator >> monitor_ysql_dimcurrency_spark_operator
    dimcurrency_gold()

dag = example_gold()