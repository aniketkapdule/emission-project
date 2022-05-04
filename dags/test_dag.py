from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args={
    'owner':'airflow',
    "depends_on_past":False,
    'start_date':days_ago(0,0,0,0,0),
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='test dag',
    schedule_interval=timedelta(days=1)
)

def just_test():
    print("It's Working")

run_it = PythonOperator(
    task_id='test_dag',
    python_callable=just_test,
    dag=dag
)

run_it