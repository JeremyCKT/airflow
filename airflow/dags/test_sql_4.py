from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 5),
    #'schedule_interval': '@daily',
    'schedule_interval': '@once',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('mysql_dag_test_0705', 
          default_args=default_args, 
          schedule_interval=None
          )

select_TP_task = MySqlOperator(
    task_id='select_TP_task',
    mysql_conn_id='MySQLWB',
    sql='select_TP_task.sql',
    dag=dag
)

select_NTP_task = MySqlOperator(
    task_id='select_NTP_task',
    mysql_conn_id='MySQLWB',
    sql='select_NTP_task.sql',
    dag=dag
)

select_KH_task = MySqlOperator(
    task_id='select_KH_task',
    mysql_conn_id='MySQLWB',
    sql='select_KH_task.sql',
    dag=dag
)

select_TN_task = MySqlOperator(
    task_id='select_TN_task',
    mysql_conn_id='MySQLWB',
    sql='select_TN_task.sql',
    dag=dag
)

select_TP_task >> select_NTP_task >> select_KH_task >> select_TN_task