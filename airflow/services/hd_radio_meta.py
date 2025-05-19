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
from common import get_client, get_logger
from deenum import IngestQueries

logger = get_logger("INGESTION-01")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process source query args")
    parser.add_argument("source", help="the name of the source")
    args = parser.parse_args()

    if args.source:
        source = args.source
        try:
            client = get_client()
            client.command(IngestQueries.CREAT_MUSICRAW.value)

            result = client.query(IngestQueries.get_source(source))

            if result.row_count > 0:
                # this always returns as a list even when you use limit 1
                result_set = result.result_set
                url = result_set[0][0]
                params = json.loads(result_set[0][1])
                headers = json.loads(result_set[0][2])

                # Main meta query
                try:
                    res = requests.get(url, headers=headers, params=params)
                    data = res.json()['data'][0]
                except Exception as e :
                    logger.error(f"Error: {e}, Response: {res}")
                    raise Exception(e)

                # Commercial check: commercials use artist as the '<station> <channel>' format
                artist = str(data['track']['artist'])
                if artist and 'HD Radio' not in artist:

                    # Check if the song hasn't ended: no dupe .
                    # -> under the assumption that no same song will be replayed within short period of time.
                    # -> AND the assumption that songs are normally under 10 min-long.
                    try:
                        res = client.query(IngestQueries.play_status(str(data['track']['title']).replace("'", "\\'")))
                        if not res.result_rows and data['song']:
                            title = str(data['track']['title'])
                            aud = data['listeners']
                            source = data['title']
                            album = str(data['track']['album'])
                            cover = data['track']['imageurl']
                            playlist = data['track'].get('playlist')
                            tag = playlist['title'] if playlist else ""
                            stream = data['tuneinurl']
                            genre = data['title'].split('-')[1].strip()
                            
                            row = [[title, aud, source, album, artist, cover, tag, stream, genre]]
                            col = ['title', 'aud', 'source', 'album', 'artist', 'cover', 'tag', 'stream', 'genre']
                            
                            # Let's assume CH insert method implements escaping single quotes...
                            client.insert('music_raw', row, column_names=col)

                        else:
                            logger.warning(f"{res.result_rows[0][0]} was already added not 10 minutes ago.")
                    except Exception as e:
                        logger.error(f"Worker logic error: {e}\nHere is the data received from request: \n{data}")
                        raise Exception(f"Worker logic error: \n{e}")
                    res.close()
                else:
                    logger.warning("This is a commercial segment. Passing.")

                result.close()
                client.close()
            else:
                result.close()
                client.close()
                logger.error("Zero row returned. Something wrong with the source name?")
                raise Exception("Zero row returned. Something wrong with the source name?")
        
        except Exception as e:
            logger.error(f"SERVICE WORKER ERROR: {e}")
            raise Exception(f"SERVICE WORKER ERROR: {e}")
    else:
        logger.error("Pass the script path as argument, you will.")
        raise Exception("Pass the script path as argument, you will.")