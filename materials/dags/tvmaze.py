import json
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

    for val in transmission_tv:
        try:
            processed_fact = json_normalize(
                {
                    "country_name": val["show"]["network"]["country"]["name"],
                    "country_code": val["show"]["network"]["country"]["code"],
                    "country_timezone": val["show"]["network"]["country"]["timezone"],
                    "webChannel_name": val["show"]["network"]["name"],
                    "show_id": val["show"]["id"],
                    "show_name": val["show"]["name"],
                    "season": val["season"],
                    "episode_number": val["number"],
                    "episode_name": val["name"],
                    "airdate": val["airdate"],
                    "airstamp": val["airstamp"],
                    "runtime": val["runtime"],
                    "show_url": val["show"]["url"],
                }
            )
            processed_fact.to_csv(
                "/tmp/process_transmission_tv.csv", index=True, header=False
            )
        except:
            print("An exception occurred")


def _store_transmission_tv():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY transmission_tv FROM stdin WITH DELIMITER ',' ",
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
            id INTEGER PRIMARY KEY,
            country_name TEXT NOT NULL,
            country_code TEXT NOT NULL,
            country_timezone TEXT NOT NULL,
            webChannel_name TEXT NOT NULL,
            show_id INTEGER NOT NULL,
            show_name TEXT NOT NULL,
            season INTEGER NOT NULL,
            episode_number INTEGER NOT NULL,
            episode_name TEXT NOT NULL,
            airdate DATE NOT NULL,
            airstamp TIMESTAMP NOT NULL,
            runtime INTEGER NOT NULL,
            show_url TEXT NOT NULL
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
