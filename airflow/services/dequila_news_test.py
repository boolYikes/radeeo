# A stowaway DAG for other project.
# I didn't want to deploy another airflow instance thats all ....
# For use in dockeroperator
import requests
import json
import argparse
from common import get_els_client, get_logger
from deenum import IngestQueries
from dotenv import load_dotenv
import os

logger = get_logger("DEQUILA-TEST-CRYPTO-PANIC")

if __name__ == "__main__":
    # Get ticker name as argument
    parser = argparse.ArgumentParser(description="Processes each crypto")
    parser.add_argument("ticker", help="the ticker of the crypto")
    args = parser.parse_args()

    ### Put these in an enum ###
    # 4 tickers: BTC, ETH, DOGE, SOL
    load_dotenv()
    api_key = json.loads(os.environ['CRYPTO_PANIC_KEY'])
    url = f"https://cryptopanic.com/api/v1/posts/?auth_token={api_key}"
    # Query param static settings
    qp1 = "&public=true"
    # filter=(rising|hot|bullish|bearish|important|saved|lol): -> no need. btw, bullish votes: good bearish votes: bad
    qp2 = f"&currencies={args.ticker}"
    qp3 = "&approved=true"

    resp = requests.get(url + qp1 + qp2 + qp3)
    data = resp.json()

    # Rate limited to 5 reqs a sec. We need 4 tickers.
    # For each ticker, paginate from 0 to n as a taskgroup.
    # they only provide the day's OR past 24 hourse worth issues So historical data is a no go.
    # -> We'll need some dataset for analysis and DL or to accumulate data.
    # For each page, there are issues in data['results'] as a list.
    # Each can either be a news or a media(e.g. YT) and has ids and urls.
    # Dupe check issue['id'] for dupe when ingesting
    # Check issue['source']['url] for original news -> this is a paid feature but i can use it apparently wg??
    # Checking dupe id should be enough for ingestion?
    # -> if so, dump them into els. Logstash can handle it