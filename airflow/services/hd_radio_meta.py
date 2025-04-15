# attention attention - shinedown
# can you roll her - ufo
# 'The HU - Yuve Yuve Yu',
# 'Triumph - Never Surrender'
# "Rick Springfield - Jessie's Girl"
# Run-D.M.C. - Walk This Way
# Run-D.M.C. - It's Tricky
# Metallica - Mama Said
# Buckcherry - Crazy Bitch

# # # # # # # # # # # # # # # # # # # #
# THIS ONLY ACCEPTS HD RADIO CHANNELS #
# # # # # # # # # # # # # # # # # # # # 

import requests
import json
import argparse
from common import get_client
import logging

# For debugging
logging.basicConfig(
    filename='/workspace/servicelogs/run_log.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("TESTING logging!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process source query args")
    parser.add_argument("source", help="the name of the source")
    args = parser.parse_args()


    if args.source:
        source = args.source
        try:
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
                    stream  String,
                    genre   String
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

                # Main meta query
                res = requests.get(url, headers=headers, params=params)
                data = res.json()['data'][0]

                # Commercial check: commercials use artist as the '<station> <channel>' format
                if 'HD Radio' not in data['track']['artist']:

                    # Check if the song hasn't ended: no dupe .
                    # -> under the assumption that no same song will be replayed within short period of time.
                    # -> AND the assumption that songs are normally under 10 min-long.
                    res = client.query(
                        f"""
                        SELECT
                        title
                        FROM music_raw
                        WHERE title = '{data['track']['title']}'
                        AND pdate >= now() - INTERVAL 10 MINUTE
                        """
                    )
                    if not res.result_rows:
                        title = data['track']['title']
                        aud = data['listeners']
                        source = data['title']
                        album = data['track']['album']
                        artist = data['track']['artist']
                        cover = data['track']['imageurl']
                        tag = data['track']['playlist']['title']
                        stream = data['tuneinurl']
                        genre = data['title'].split('-')[1].strip()
                        
                        row = [[title, aud, source, album, artist, cover, tag, stream, genre]]
                        col = ['title', 'aud', 'source', 'album', 'artist', 'cover', 'tag', 'stream', 'genre']

                        client.insert('music_raw', row, column_names=col)

                    else:
                        logging.warning(f"{res.result_rows[0][0]} was already added not 10 minutes ago.")
                    res.close()
                else:
                    logging.warning("This is a commercial segment. Passing.")

                result.close()
                client.close()
            else:
                result.close()
                client.close()
                logging.error("Zero row returned. Something wrong with the source name?")
                raise Exception("Zero row returned. Something wrong with the source name?")
        
        except Exception as e:
            print(f"SERVICE WORKER ERROR: {e}")
    else:
        logging.error("Pass the script path as argument, you will.")
        raise Exception("Pass the script path as argument, you will.")