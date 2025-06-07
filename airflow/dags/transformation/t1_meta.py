from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import pendulum

DAG_NAME = 't1_meta_transformation'

with DAG(
    dag_id=DAG_NAME,
    start_date=pendulum.datetime(2025, 6, 7, tz="UTC"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["all", "t1", "meta"],
    description="The first metadata transformation layer for all sources",
    max_active_runs=1,
    max_active_tasks=1
) as dag: 
    
    t1_meta_transformation = DockerOperator(
        task_id='t1_meta_transformation',
        image='radeeo-python-service:latest',
        command=["-c", f"""python meta_transformer.py || exit 1"""], # -u makes stdout unbuffered
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
    
    t1_meta_transformation