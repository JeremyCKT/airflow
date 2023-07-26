from datetime import datetime
from airflow import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
#import requests
#from bs4 import BeautifulSoup

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 6),
    'retries': 3,
}


dag = DAG('create_table', default_args=default_args, schedule_interval='@once')
#with DAG('create_table', default_args=default_args, schedule_interval='@once') as dag:

#blog_scraper = BlogScraper()

create_table = MySqlOperator(
    task_id = 'create_table',
    mysql_conn_id='MySQLWB',
    sql="create_table.sql",
    dag=dag,
)

create_table