from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

default_args = {
    'start_date': days_ago(1),
}

def _training_model():
    return 0.7

def _check_accuracy(ti):
    accuracy = ti.xcom_pull(task_id='training_model')
    if (accuracy < 0.5):
        return ['is_inaccurate','send_slack']
    return 'is_accurate'

@dag(schedule_interval='@daily', default_args=default_args, catchup=False, tags=['astronomer', 'branch'])
def astronomer_branchpythonoperator():

    training_model = DummyOperator(task_id='training_model',python_callable=_training_model)
    check_acc = BranchPythonOperator(task_id='check_accuracy', python_callable=_check_accuracy,do_xcom_push=False)
    is_accurate = DummyOperator(task_id='is_accurate')
    is_inaccurate = DummyOperator(task_id='is_inaccurate')
    send_slack = DummyOperator(task_id='send_slack')
    store_acc = DummyOperator(task_id='store_acc',trigger_rule='none_failed_or_skipped')


    training_model >> check_acc >> [is_accurate,is_inaccurate, send_slack]
    [is_accurate,is_inaccurate] >> store_acc
dag = astronomer_branchpythonoperator()