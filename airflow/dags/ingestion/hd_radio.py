from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from services.operators.local_sensor import LocalFileSensor
from services.common import json_load_wrapper, key_sanitizer
from docker.types import Mount
import pendulum
import os
import time

DAG_NAME = 'hd_radio_ingestion'

with DAG(
    dag_id=DAG_NAME,
    start_date=pendulum.datetime(2025, 4, 14, tz="UTC"),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["radio", "hd_radio"],
    description="Stream metadata ingestion layer for the radio station 'HD Radio'",
    max_active_runs=1,
    max_active_tasks=8
) as dag: 
    
    wait_for_sorting = LocalFileSensor(
        task_id="wait_for_sorting",
        filepath=os.path.join(os.environ["AIRFLOW_HOME"], "services/sources.json"),
        poke_interval=20,
        timeout=300,
        mode="reschedule"
    )

    with TaskGroup("hd_radio_group") as hd_radio_group:
        for source in json_load_wrapper(os.path.join(os.environ["AIRFLOW_HOME"], "services/sources.json")):
            if source['source'] == 'HD Radio':

                ingest_radio_info = DockerOperator(
                    task_id=key_sanitizer(f"ingest_{source['sname']}"),
                    image='radeeo-python-service:latest',
                    command=["-c", f"""python hd_radio_meta.py '{source["sname"]}' || exit 1"""], # -u makes stdout unbuffered
                    docker_url='unix://var/run/docker.sock',
                    working_dir='/workspace',
                    extra_hosts={'host.docker.internal':'host-gateway'},
                    xcom_all=True, 
                    tty=False, # needed to see logs from the container
                    network_mode='bridge', 
                    auto_remove='force',
                    mounts=[Mount(source='/lab/dee/repos_side/radeeo/airflow/services', target='/workspace', type="bind")],
                    mount_tmp_dir=False
                )

                # ingest_radio_info
                time.sleep(1)

    wait_for_sorting >> hd_radio_group