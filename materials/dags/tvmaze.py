import json
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pandas import json_normalize

def _process_transmission_tv(ti):
    transmission_tv = ti.xcom_pull(task_ids="extract_transmission")

    df = pd.DataFrame(transmission_tv)
    df = df[['id', 'name', 'season', 'number', 'airdate', 'airstamp', 'runtime', 'url']]

    df.to_csv("/tmp/process_transmission_tv.csv", index=False, header=False, sep="\002")

def _store_transmission_tv():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="""COPY transmission_tv FROM stdin WITH DELIMITER '\002' """,
        filename="/tmp/process_transmission_tv.csv",
    )

with DAG(
    "tvmaze", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False
) as dag:

    create_table = PostgresOperator(
        postgres_conn_id="postgres",
        task_id="create_table",
        sql="""
        CREATE TABLE IF NOT EXISTS transmission_tv(
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            season INTEGER,
            number INTEGER,
            airdate DATE,
            airstamp TIMESTAMP,
            runtime TEXT,
            url TEXT NOT NULL
        );""",
    )

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="tvmaz_api",
        endpoint="schedule/",
    )

    extract_transmission = SimpleHttpOperator(
        task_id="extract_transmission",
        http_conn_id="tvmaz_api",
        endpoint="schedule/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    process_transmission_tv = PythonOperator(
        task_id="process_transmission_tv", python_callable=_process_transmission_tv
    )

    store_transmission_tv = PythonOperator(
        task_id="store_transmission_tv", python_callable=_store_transmission_tv
    )

    (
        create_table
        >> is_api_available
        >> extract_transmission
        >> process_transmission_tv
        >> store_transmission_tv
    )