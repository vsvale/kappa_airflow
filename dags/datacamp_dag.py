from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor


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
        import time
        time.sleep(lenght_time)

    email_task = EmailOperator(task_id='Notify', to='viniciusdvale@gmail.com', subject='Datacamp dag sleep well', html_content='<p>Time to wake up little Dag<p>')

    salesdata_sensor = FileSensor(task_id='pock_salesdata',filepath='salesdata.csv',poke_interval=30, timeout=60*5, mode='reschedule')

    t_pintme = printme()
    t_sleep = sleep(5)

    rand_number >> [echo_ex, t_pintme] >> t_sleep >> email_task >> salesdata_sensor

dag = datacamp_dag()