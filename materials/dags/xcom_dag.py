from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

def _t1():
    return 10

def _t2(ti):
    data = ti.xcom_pull(task_ids="t1")

def _t3(ti):
    None

with DAG("xcom_dag", start_date=datetime(2022,1,1), schedule_interval='@daily', catchup=False) as dag:
    t1 = PythonOperator(task_id='t1', python_callable=_t1)

    t2 = PythonOperator(task_id='t2', python_callable=_t2)

    t3 = PythonOperator(task_id='t3', python_callable=_t3)

    t1 >> t2 >> t3