from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

path_temp_csv = "https://minio.deepstorage.svc.cluster.local/landing/airflow/staging.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=08XGXX1OOSDUXE2HXL1F%2F20221010%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20221010T115916Z&X-Amz-Expires=604799&X-Amz-Security-Token=eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiIwOFhHWFgxT09TRFVYRTJIWEwxRiIsImV4cCI6MTY2NTQwNjUyOCwicG9saWN5IjoiY29uc29sZUFkbWluIn0.mnLbG3PkLlsd1IEkt9bf64N7smEYv1bIh23Wf5kdHA5UPyPL9jEj368etTa1bvNX9DpPf_rfjQRPO2U3zDciWw&X-Amz-SignedHeaders=host&versionId=null&X-Amz-Signature=611614c76e7c664b3fff009d47e005ef69fc2cda40a0afe4acbd552bfb923120"
email_failed = "viniciusdvale@gmail.com"

default_args = {
    'start_date': days_ago(1),
    'email': email_failed,
    'email_on_failure':False
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

@dag(schedule=None, default_args=default_args, catchup=False, tags=['stack', 'email','pipeline'],doc_md=doc_md, description=description)
def stack_etl_pipeline_email():

    @task
    def extract():
        import pymysql
        import sqlalchemy
        import pandas as pd
        import os

        path = os.getcwd()

        print(path)

        engine = sqlalchemy.create_engine('mysql+pymysql://root:PlumberSDE@172.18.0.2:3306/employees')
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
        import pandas as pd

        df = pd.read_csv(path_temp_csv)
        df['name'] = df['first_name']+" "+df['last_name']
        print(df.head(5))
        df.drop(['emp_no','first_name','last_name'],axis=1,inplace=True)
        df.to_csv(path_temp_csv, index=False)

    @task
    def load():
        import psycopg2
        import sqlalchemy
        import pandas as pd

        engine = sqlalchemy.create_engine('postgres+psycopg2://plumber:PlumberSDE@172.18.0.3:5433/dw_employee')
        df = pd.read_csv(path_temp_csv)
        df.to_sql("d_employees",engine,if_exists="replace",index=False)

    @task
    def clean():
        import os

        try:
            os.remove(path_temp_csv)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))

    email_task = EmailOperator(task_id='Notify', to=email_failed, subject='Stack pipeline first finalizado com sucesso', html_content='<p>Salvo em d_employees<p>')

    extract()>>transform()>>load()>>clean()>>email_task

dag = stack_etl_pipeline_email()