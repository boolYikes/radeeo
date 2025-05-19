from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'DW',
    # 'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 4, 30, tz="UTC"),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

with DAG(
    dag_id="old_meta_cleanup",
    schedule="0 3 * * 0",
    catchup=True,
    tags=["system", "cleanup"],
    description="Does cleanup on old metadata and xcom, every sunday, targeting records older than a fortnight.",
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
) as dag: 
    
    clean_meta = BashOperator(
        task_id="clean_meta",
        bash_command="""
            airflow db clean \
            --clean-before-timestamp {{ macros.ds_add(ds, -14) }} \
            --skip-archive \
            --yes
        """
    )

    clean_celery_meta = SQLExecuteQueryOperator(
        task_id="clean_celery_meta",
        conn_id="airflow_db",
        sql="""
            DELETE FROM celery_taskmeta
            WHERE date_done < NOW() - INTERVAL '14 days';
        """
    )

    clean_xcom = SQLExecuteQueryOperator(
        task_id="clean_xcom",
        conn_id="airflow_db",
        sql="""
            DELETE FROM xcom
            WHERE timestamp < NOW() - INTERVAL '14 days';
        """
    )

    # Also there is a table called 'rendered_task_instance_fields' and 'celery_tasksetmeta' 
    # but they're not too heavy so I'll leave it at that
