from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pandas import json_normalize
from datetime import datetime

def _process_cat_fact(ti):
    fact = ti.xcom_pull(task_ids="extract_cat_fact")
    processed_fact = json_normalize({
        'fact': fact['fact'],
        'length': fact['length']
    })
    processed_fact.to_csv('/tmp/processed_cat_facts.csv', index=None, header=False)


def _store_cat_fact():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(sql="COPY cat_facts FROM stdin WITH DELIMITER ',' ",
                     filename='/tmp/processed_cat_facts.csv')

with DAG('cat_facts_processing', start_date=datetime(2022,1,1), schedule_interval='*/30 * * * *', catchup=False) as dag:
    create_table = PostgresOperator(
        postgres_conn_id = 'postgres',
        task_id = 'create_table',
        sql = '''
        CREATE TABLE IF NOT EXISTS cat_facts(
            fact TEXT NOT NULL,
            length INTEGER NOT NULL
        );'''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='cat_fact_api',
        endpoint='fact/'
    )

    extract_cat_fact = SimpleHttpOperator(
        task_id='extract_cat_fact',
        http_conn_id='cat_fact_api',
        endpoint='fact/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_cat_fact = PythonOperator(
        task_id='process_cat_fact',
        python_callable=_process_cat_fact
    )

    store_cat_fact = PythonOperator(
        task_id='store_cat_fact',
        python_callable=_store_cat_fact
    )

    create_table >> is_api_available >> extract_cat_fact >> process_cat_fact >> store_cat_fact