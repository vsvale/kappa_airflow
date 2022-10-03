from operator import index
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.operator.email_operator import EmailOperator
import pandas as pd
import sqlalchemy
import os

path_temp_csv = "/tmp/staging.csv"
email_failed = "viniciusdvale@gmail.com"

default_args = {
    'start_date': days_ago(1),
    'email': email_failed,
    'email_on_failure':True
}

doc_md = """
### DAG
#### Desciption
Pipeline para o processo de ETL dos ambientes de produção oltp ao olap
#### Source
Mysql on K8s
**Database**: employees

#### Destination
Yugabyte on K8s
**Database**: dw_employee
"""

description = "Pipeline para o processo de ETL dos ambientes de produção oltp ao olap"

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['stack', 'email'],doc_md=doc_md, description=description)
def stack_etlpipelinefirst():

    @task
    def extract():
        engine = sqlalchemy.create_engine('mysql+pymysql://root:PlumberSDE@172.20.0.2:3306/employees')

        df = pd.read_sql_query(r"""
                select
                emp.emp_no,
                emp.first_name,
                emp.last_name,
                sal.salary,
                titles.title
                from employees emp
                inner join (select emp_no, max(salary) as salary from salaries group by emp_no) sal
                on sal.emp_no = emp.emp_no
                inner join titles
                on titles.emp_no = emp.emp_no limit 100; """
                ,engine)
        
        df.to_csv(path_temp_csv, index=False)

    @task
    def transform():
        df = pd.read_csv(path_temp_csv)

        df['name'] = df['first_name']+" "+df['last_name']

        df.drop(['emp_no,first_name,last_name'],axis=1,inplace=True)

        df.to_csv(path_temp_csv, index=False)

    @task
    def load():
        engine = sqlalchemy.create_engine('postgres+psycopg2://plumber:PlumberSDE@172.20.0.2:5433/dw_employee')

        df = pd.read_csv(path_temp_csv)

        df.to_sql("d_employees",engine,if_exists="replace",index=False)

    @task
    def clean():
        try:
            os.remove(path_temp_csv)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))

    email_task = EmailOperator(task_id='Notify', to=email_failed, subject='Stack pipeline first finalizado com sucesso', html_content='<p>Salvo em d_employees<p>')

    extract()>>transform()>>load()>>clean()>>email_task

dag = stack_etlpipelinefirst()