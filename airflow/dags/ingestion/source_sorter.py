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

    # The entrypoint of the image is ["/bin/bash"]
    generate_source = DockerOperator(
        task_id='generate_source',
        image='python3.12_service:latest',
        command=['-c', 'python source_sorter.py || exit 1'],
        docker_url='unix://var/run/docker.sock',
        working_dir='/workspace',
        extra_hosts={'host.docker.internal':'host-gateway'},
        xcom_all=True, # for debugging (stdout to airflow)
        network_mode='bridge', # this is prolly unnecessary
        auto_remove='force',
        # mounts=[Mount('/workspace', 'service')],
        mounts=[Mount(source='/lab/dee/repos_side/radeeo/airflow/services', target='/workspace', type="bind")],
        mount_tmp_dir=False # this is important ...
    )
 
    generate_source