from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 4)
}

dag = DAG('my_database_dag_2', default_args=default_args, schedule_interval=None)

create_table_task = MySqlOperator(
    task_id='create_table_task',
    mysql_conn_id='airflow_db',
    sql='CREATE TABLE my_table (id INT, name VARCHAR(255))',
    dag=dag
)

insert_data_task = MySqlOperator(
    task_id='insert_data_task',
    mysql_conn_id='airflow_db',
    sql='INSERT INTO my_table VALUES (1, "John")',
    dag=dag
)

create_table_task >> insert_data_task