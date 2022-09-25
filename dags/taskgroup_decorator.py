from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from datetime import datetime

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False)
def taskgroup_decorator():

    @task.python
    def task_a(value):
        return value + 10

    @task.python
    def task_b(value):
        return value + 20

    @task.python
    def task_c(value):
        return value + 30

    @task_group(group_id="tasks")
    def tasks():
        return task_a(task_b(task_c(10)))
    tasks()
dag = taskgroup_decorator()