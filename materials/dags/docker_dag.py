from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from datetime import datetime

@dag(start_date=datetime(2022,1,1), schedule_interval='@daily', catchup=False)

def docker_dag():

    @task()
    def t1():
        pass

    t2 = DockerOperator(task_id='t2',
                        image='python:3.9.9-slim-buster',
                        command='echo "command running in docker container"',
                        #docker_url='unix://var/run/docker.sock',
                        network_mode='bridge',
                        api_version="auto",
                        auto_remove=True)

    t1() >> t2

dag = docker_dag()