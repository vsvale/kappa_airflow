from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['stack', 'branch','taskflow'])
def stack_branchoperator():

    @task
    def get_acc_op():
        return 90
    
    @task.branch()
    def check_accuracy(acc):
        acc_value = int(acc)
        if acc_value >=90:
            return 'deploy_task'
        return 'retrain_task'

    deploy_op = EmptyOperator(task_id='deploy_task')
    retrain_op = EmptyOperator(task_id='retrain_task')

    check_accuracy(get_acc_op())>>[deploy_op,retrain_op]
dag = stack_branchoperator()