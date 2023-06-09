import requests
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago


def get_api_data(**kwargs):
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    data = response.json()
    for user in data:
        if user["id"] == 4:
            data_dict = transform(user)
            kwargs['ti'].xcom_push(key='data_from_api', value=data_dict)
            #df = pd.DataFrame(data_dict)
            #df.to_csv('/tmp/user.csv', sep='\t', index=False, header=False)

def transform(user):
    keys = ["id","name", "username","email"]
    new_dict = {key: user[key] for key in keys if key in user}
    return new_dict

def save_to_mysql(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data_from_api')
    mysql_hook = MySqlHook(mysql_conn_id='mysql_webinar')
    name = data["name"]
    username = data["username"]
    email = data["email"]
    mysql_hook.run(f"INSERT INTO users (name, username, email) VALUES ('{name}', '{username}', '{email}');")


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
    'workflow',
    default_args=default_args,
    description='A DAG that get data from a public API and send to Database',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['3rd_dag']
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

get_data = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_api_data,
    provide_context=True,
)
    
save_data = PythonOperator(
    task_id='save_data_to_mysql',
    python_callable=save_to_mysql,
    provide_context=True,
)

end_task = EmptyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

start_task >> get_data >> save_data >> end_task

