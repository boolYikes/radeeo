from dotenv import load_dotenv
import clickhouse_connect
from elasticsearch import Elasticsearch
import os
import json
from typing import List
import re
import logging
from datetime import datetime, timezone, timedelta

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

def get_els_client():
    """
    Gets connections for Elasticsearch server.
    """
    load_dotenv()
    pw = json.loads(os.environ['ELS_PW'])
    user = json.loads(os.environ['ELS_USER'])
    host = json.loads(os.environ['ELS_HOST'])
    cert = json.loads(os.environ['ELS_CERT'])
    
    # ASSERT HOSTNAME IN PRODUCTION!!!!
    client = Elasticsearch(
        host, 
        basic_auth=(user, pw), 
        ca_certs=cert, 
        verify_certs=True, ssl_assert_hostname=False
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
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    formatter.converter = custom_time
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logging.basicConfig(
        filename=f'/workspace/servicelogs/{logging_entity_name}.log',
        filemode='a',
        level=logging.ERROR
    )

    logger = logging.getLogger(logging_entity_name)
    logger.addHandler(handler)
    return logger

def custom_time():
    tz = timezone(timedelta(hours=9))
    return datetime.now(tz)