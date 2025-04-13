# attention attention - shinedown
# can you roll her - ufo
# 'The HU - Yuve Yuve Yu',
# 'Triumph - Never Surrender'
# "Rick Springfield - Jessie's Girl"
# Run-D.M.C. - Walk This Way
# Run-D.M.C. - It's Tricky
# Metallica - Mama Said
# Buckcherry - Crazy Bitch
import requests
import clickhouse_connect
import os
import json
import argparse
from typing import List

def get_client():
    conn = json.loads(os.environ['CLICKHOUSE_CONN'])
    client = clickhouse_connect.get_client(
        host=conn['host'],
        port=conn['port'],
        username=conn['username'],
        password=conn['password'],
        database=conn['database'],
    )
    return client

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process source query args")
    parser.add_argument("source", help="the name of the source")
    args = parser.parse_args()
    source = args.source

    client = get_client()
    client.command(
        """
        CREATE TABLE IF NOT EXISTS music_raw
        (
            title   String,
            pdate   Datetime default now(),
            aud     Int32,
            source  String,
            album   String,
            artist  String,
            cover   String,
            tag     String,
            stream  String
        )
        ENGINE = MergeTree
        ORDER BY (pdate)
        """
    )

    result = client.query(
        f"""
        SELECT url, params, headers
        FROM source_meta
        WHERE sname = '{source}'
        ORDER BY added_on desc
        LIMIT 1
        """
    )

    if result.row_count > 0:
        # this always returns as a list even when you use limit 1
        result_set = result.result_set
        url = result_set[0][0]
        params = json.loads(result_set[0][1])
        headers = json.loads(result_set[0][2])

        # main meta query
        # This needs to be a dynamic schema detection. upgrade it later.
        res = requests.get(url, headers=headers, params=params)
        data = res.json()['data'][0]
        
        title = data['track']['title']
        aud = data['listeners']
        source = data['title']
        album = data['track']['album']
        artist = data['track']['artist']
        cover = data['track']['imageurl']
        tag = data['track']['playlist']['title']
        stream = data['tuneinurl']
        
        row = [[title, aud, source, album, artist, cover, tag, stream]]
        col = ['title', 'aud', 'source', 'album', 'artist', 'cover', 'tag', 'stream']

        client.insert('music_raw', row, column_names=col)

        result.close()
        client.close()
    else:
        result.close()
        client.close()
        raise Exception("Zero row returned. Something wrong with the source name?")