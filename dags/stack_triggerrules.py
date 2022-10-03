from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['stack', 'branch','taskflow','triggerrule'])
def stack_triggerrule():

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
    notify_op = EmptyOperator(task_id='notify_task',trigger_rule=TriggerRule.NONE_FAILED)
    check = check_accuracy(get_acc_op())
    
    check >> Label("Acc >= 90%") >>deploy_op
    check >> Label("Acc < 90%") >>retrain_op
    [deploy_op,retrain_op] >> notify_op
dag = stack_triggerrule()