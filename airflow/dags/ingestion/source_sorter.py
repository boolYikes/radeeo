from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum
from docker.types import Mount

with DAG(
    dag_id="source_extractor",
    start_date=pendulum.datetime(2025, 4, 13, tz="UTC"),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["source_sorter"],
) as dag: 

    generate_source = DockerOperator(
        task_id='generate_source',
        image='python3.12_service:latest',
        command='python source_sorter.py',
        network_mode='airflow_default', # this is prolly unnecessary
        auto_remove='force',
        mounts=[Mount('/workspace', 'service')]
    )
 
    generate_source