from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from services.operators.local_sensor import LocalFileSensor
from docker.types import Mount
import pendulum
import os
import json
import time

with DAG(
    dag_id="hd_radio_ingestion",
    start_date=pendulum.datetime(2025, 4, 14, tz="UTC"),
    schedule_interval="* * * * *",
    catchup=False,
    tags=["hd_radio"],
) as dag: 
    
    wait_for_sorting = LocalFileSensor(
        task_id="wait_for_sorting",
        filepath=os.path.join(os.environ["AIRFLOW_HOME"], "services/sources.json"),
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    with TaskGroup("hd_radio_group") as hd_radio_group:
        with open(os.path.join(os.environ["AIRFLOW_HOME"], "services/sources.json"), 'r') as f:
            for source in json.load(f):
                if source['source'] == 'HD Radio':
                    ingest_radio_info = DockerOperator(
                        task_id=f"ingest_{source['sname']}",
                        image='python3.12_service:latest',
                        command=f'python hd_raio_meta.py {source['sname']}',
                        network_mode='airflow_default',
                        auto_remove='force',
                        mounts=[Mount(source='/lab/dee/repos_side/radeeo/airflow/services', target='/workspace', type="bind")]
                    )

                    ingest_radio_info
                    time.sleep(1)

    wait_for_sorting >> hd_radio_group