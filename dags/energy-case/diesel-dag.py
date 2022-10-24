# [pre_requisites]
# create connectivity to kubernetes on airflow ui [connections]
# create connectivity to minio on airflow ui [connections]
# create connectivity to yugabyte on airflow ui [connections]

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'vale',
    'start_date': datetime(2022, 10, 18),
    'depends_on_past': False,
    'email': ['viniciusdvalea@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'max_active_runs':1
    }

@dag(schedule='@daily', default_args=default_args, catchup=False, tags=['energycase ', 'plumbers','api'])
def energy_case():
    landing_diesel = SparkKubernetesOperator(
        task_id = 'landing_diesel',
        namespace='processing',
        application_file='diesel-to-landing.yaml',
        kubernetes_conn_id='kubeconnect',
        do_xcom_push=True
    )
    monitor_landing_diesel = SparkKubernetesSensor(
        task_id = 'monitor_landing_diesel',
        namespace='processing',
        application_name="{{ task_instance.xcom_pull(task_ids='landing_diesel')['metadata']['name'] }}",
        kubernetes_conn_id="kubeconnect"
    )

    landing_diesel >> monitor_landing_diesel
dag = energy_case()