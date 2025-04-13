from dotenv import load_dotenv
import clickhouse_connect
import os
import json
from typing import List

def get_client():
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