import json
from datetime import datetime

import pandas as pd
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
    df = df[["id", "name", "season", "number", "airdate", "airstamp", "url"]]

    df.to_csv("/tmp/process_transmission_tv.csv", index=False, header=False, sep="\002")


def _store_transmission_tv():
    hook = PostgresHook(postgres_conn_id="database")
    hook.copy_expert(
        sql="""COPY transmission_tv FROM stdin WITH DELIMITER '\002' """,
        filename="/tmp/process_transmission_tv.csv",
    )


with DAG(
    "tvmaze", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False
) as dag:

    create_table = PostgresOperator(
        postgres_conn_id="database",
        task_id="create_table",
        sql="""
        CREATE TABLE IF NOT EXISTS transmission_tv(
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            season TEXT,
            number TEXT,
            airdate DATE,
            airstamp TIMESTAMP,
            url TEXT NOT NULL
        );""",
    )

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="tvmaz_api",
        endpoint="schedule/web?date=2022-09-05&country=RU",
    )

    extract_transmission = SimpleHttpOperator(
        task_id="extract_transmission",
        http_conn_id="tvmaz_api",
        endpoint="schedule/web?date=2022-09-05&country=RU",
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

    create_table_analysis = PostgresOperator(
        postgres_conn_id="database",
        task_id="create_table_analysis",
        sql="""
        CREATE TABLE IF NOT EXISTS count_transmissions(
            airdate DATE NOT NULL,
            count INTEGER
        );""",
    )

    store_count_series = PostgresOperator(
        postgres_conn_id="database",
        task_id="store_count_series",
        sql="""
        INSERT INTO count_transmissions
            SELECT airdate, COUNT(id)
            FROM transmission_tv
            GROUP BY airdate
        ;""",
    )

    truncate_table_analysis = PostgresOperator(
        postgres_conn_id="database",
        task_id="truncate_table_analysis",
        sql="""TRUNCATE TABLE transmission_tv;""",
    )

    (
        create_table
        >> is_api_available
        >> extract_transmission
        >> process_transmission_tv
        >> store_transmission_tv
        >> create_table_analysis
        >> store_count_series
        >> truncate_table_analysis
    )
