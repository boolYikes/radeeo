from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from services.common import key_sanitizer
from plugins.termux_alert import vibrate_alert
from docker.types import Mount
import pendulum

DAG_NAME = 'riw_vintage_ingestion'
STATION_NAME = 'RIW Vintage Channel'

default_args = {
    "owner": "dee",
    "retries": "0",
    "on_failure_callback": vibrate_alert
}

with DAG(
    dag_id=DAG_NAME,
    start_date=pendulum.datetime(2025, 4, 14, tz="UTC"),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["radio", "getmeradio.com", "riw_vintage"],
    description="Stream metadata ingestion layer for the radio station 'RIW Vintage Channel'",
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args
) as dag: 
    
    # No substation -> no file setnitnel
    ingest_radio_info = DockerOperator(
        task_id=key_sanitizer("ingest_riw_vintage"),
        image='radeeo-python-service:latest',
        command=["-c", f"""python getmeradio_meta.py '{STATION_NAME}' || exit 1"""],
        docker_url='unix://var/run/docker.sock',
        working_dir='/workspace',
        extra_hosts={'host.docker.internal':'host-gateway'},
        xcom_all=True, 
        tty=False,
        network_mode='bridge',
        auto_remove='force',
        mounts=[Mount(source='/lab/dee/repos_side/radeeo/airflow/services', target='/workspace', type="bind")],
        mount_tmp_dir=False
    )