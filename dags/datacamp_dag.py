from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
import time

default_args = {
    'start_date': days_ago(1),
    'owner':'vale',
    'email':'viniciusdvale@gmail.com'

}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['datacamp'])
def datacamp_dag():
    rand_number = BashOperator(task_id='rand_number',bash_command='echo $RANDOM')
    echo_ex = BashOperator(task_id='echo_ex',bash_command='echo "Exemple!"')

    @task
    def printme():
        print("This goes in the logs!")

    @task
    def sleep(lenght_time):
        time.sleep(lenght_time)

    email_task = EmailOperator(task_id='Notify', to='viniciusdvale@gmail.com', subject='Datacamp dag sleep well', html_content='<p>Time to wake up little Dag<p>')

    t_pintme = printme()
    t_sleep = sleep(5)

    rand_number >> [echo_ex, t_pintme] >> t_sleep >> email_task

dag = datacamp_dag()