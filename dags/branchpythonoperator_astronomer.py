from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'start_date': days_ago(1),
}

def _check_accuracy():
    accuracy = 0.7
    if (accuracy < 0.5):
        return 'is_inaccurate'
    return 'is_accurate'

@dag(schedule_interval='@daily', default_args=default_args, catchup=False)
def branchpyoperator():

    training_model = DummyOperator(task_id='training_model')
    is_accurate = DummyOperator(task_id='is_accurate')
    is_inaccurate = DummyOperator(task_id='is_inaccurate')
    check_accuracy = BranchPythonOperator('check_accuracy', python_callable=_check_accuracy)
    training_model >> check_accuracy >> [is_accurate,is_inaccurate]
dag = branchpyoperator()