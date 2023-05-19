import requests
import json
from pandas import json_normalize
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


def get_api_data(user_id):
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    data = response.json()
    for user in data:
        if user["id"] == user_id:
            print(user)

#def transform_data():

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A DAG that get data from a public API and send to Database',
    schedule_interval=None,
    start_date=days_ago(2),
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

get_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_api_data,
    op_args=[4],
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

start_task >> get_data_task >> end_task
