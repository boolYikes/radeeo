from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import pendulum
from datetime import timedelta
from docker.types import Mount

with DAG(
    dag_id="source_extractor",
    start_date=pendulum.datetime(2025, 4, 14, tz="UTC"),
    schedule=timedelta(days=2),
    catchup=False,
    tags=["source_sorter"],
    description="It periodically checks new sources and outputs to a json.",
    max_active_runs=1,
    max_active_tasks=1
) as dag: 

    # The entrypoint of the image is ["/bin/bash"]
    generate_source = DockerOperator(
        task_id='generate_source',
        image='python3.12_service:latest',
        command=['-c', 'python source_sorter.py || exit 1'], # exit code needed lest it will silent-fail
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