import requests
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def fetch_and_process_data():
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    data = response.json()
    print(f"Fetched {len(data)} users.")
    for user in data:
        print(f"User ID: {user['id']}, Name: {user['name']}")


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
    'fetch_data_dag',
    default_args=default_args,
    description='A DAG that fetches data from a public API',
    schedule_interval=None,
    start_date=days_ago(2),
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_and_process_data,
    dag=dag,
)
