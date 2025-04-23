from dotenv import load_dotenv
import clickhouse_connect
import os
import json
from typing import List
import re
import logging

def get_client():
    """
    Gets DB connection to the Clickhouse server.
    """
    load_dotenv()
    conn = json.loads(os.environ['CLICKHOUSE_CONN'])
    client = clickhouse_connect.get_client(
        host=conn['host'],
        port=conn['port'],
        username=conn['username'],
        password=conn['password'],
        database=conn['database'],
    )
    return client

def json_load_wrapper(path: str) -> List:
    """
    This function pre-loads json at parse-time so that parsing doesn't fail due to non-existant json.
    """
    if not os.path.exists(path):
        return []
    with open(path, 'r') as f:
        return json.load(f)

def key_sanitizer(task_id: str) -> str:
    task_id = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', task_id)
    task_id = task_id.strip('.')
    return task_id

def get_logger(logging_entity_name: str) -> logging.Logger:
    logging.basicConfig(
        filename=f'/workspace/servicelogs/run_log_{logging_entity_name}.log',
        filemode='a',
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    return logging.getLogger(logging_entity_name)