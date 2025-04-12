from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import clickhouse_connect
import os
import json

conn = json.loads(os.environ['CLICKHOUSE_CONN'])

def test_clickhouse():
    client = clickhouse_connect.get_client(
        host=conn['host'],
        port=conn['port'],
        username=conn['username'],
        password=conn['password'],
        database=conn['database'],
    )
    client.command('SELECT timezone()')

with DAG(
    dag_id="clickhouse_create_table",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["clickhouse"],
) as dag: 

    test_task = PythonOperator(
        task_id='test_clickhouse',
        python_callable=test_clickhouse
    )

    test_task