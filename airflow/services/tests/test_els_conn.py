from datetime import datetime
from elasticsearch import Elasticsearch

# Use corresponding path
with open('./pw.env', 'r') as f:
    pw = f.readlines()[0].strip()

# Assert hostname in production! Use certbot!!
client = Elasticsearch(
    "https://host.docker.internal:9200/", 
    basic_auth=("elastic", pw), 
    ca_certs="/opt/airflow/services/http_ca.crt", 
    verify_certs=True, ssl_assert_hostname=False
)

doc = {
    "author": "kimchy",
    "text": "Elasticsearch: cool. bonsai cool.",
    "timestamp": datetime.now(),
}

resp = client.index(index="test-index", id=1, document=doc)

print(resp["result"])