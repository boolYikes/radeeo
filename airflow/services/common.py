from dotenv import load_dotenv
import clickhouse_connect
import os
import json
from typing import List

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
