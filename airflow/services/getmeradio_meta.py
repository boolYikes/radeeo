# # # # # # # # # # # # # # # # # # # # # # #
# THIS ONLY ACCEPTS GETMERADIO.COM CHANNELS #
# # # # # # # # # # # # # # # # # # # # # # #

"""
A working curl command example
curl -X POST "https://www.getmeradio.com/common/interval.php" \
  -H "Host: www.getmeradio.com" \
  -H "User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0" \
  -H "Accept: application/json, text/javascript, */*; q=0.01" \
  -H "Accept-Language: en-US,en;q=0.5" \
  -H "Content-Type: application/x-www-form-urlencoded; charset=UTF-8" \
  -H "X-Requested-With: XMLHttpRequest" \
  -H "Origin: https://www.getmeradio.com" \
  -H "Referer: https://www.getmeradio.com/stations/70s80s90sriwvintagechannel-8195/" \
  --data "stationId=8195&nowPlaying=1"
"""

import requests
import json
import argparse
from common import get_client, get_logger
from deenum import IngestQueries

logger = get_logger("INGESTION-01")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process source query args")
    parser.add_argument("station", help="the getmeradio.com station ID")
    args = parser.parse_args()

    if args.station:
        station_name: str = args.station
        try:
            client = get_client()
            client.command(IngestQueries.CREAT_MUSICRAW.value)

            result = client.query(IngestQueries.get_source(station_name))

            if result.row_count > 0:
                result_set = result.result_set
                url, params, headers = result_set[0]

                sid, nowplaying = json.loads(params)['param'].split("&")
                dat_payload = {sid.split("=")[0]: sid.split("=")[1], nowplaying.split("=")[0]: nowplaying.split("=")[1]}
                try:
                    res = requests.post(url, headers=json.loads(headers), data=dat_payload)
                    data = res.json()[0] # only one member guaranteed
                except Exception as e :
                    logger.error(f"Error: {e}, Response: {res}")
                    raise Exception(e)

                # RIW does not broadcast commercial meta data

                # Check if the song hasn't ended: no dupe .
                # -> under the assumption that no same song will be replayed within short period of time.
                # -> AND the assumption that songs are normally under 10 min-long.
                try:
                    res = client.query(IngestQueries.play_status(str(data['title']).replace("'", "\\'")))
                    if not res.result_rows and data['title']:
                        artist = data['artist'].replace("'", "\\'")
                        title = str(data['title'])
                        aud = -1 # Does not publish
                        source = 'RIW Vintage Channel'
                        album = '' # Does not publish
                        cover = data['artwork']
                        tag = '70/80/90' # Does not provide
                        stream = '' # Points to general stream
                        genre = ''
                        
                        row = [[title, aud, source, album, artist, cover, tag, stream, genre]]
                        col = ['title', 'aud', 'source', 'album', 'artist', 'cover', 'tag', 'stream', 'genre']
                        
                        # Let's assume CH insert method implements escaping single quotes...
                        client.insert('music_raw', row, column_names=col)

                    else:
                        logger.warning("The song was already added not 10 minutes ago.")
                except Exception as e:
                    logger.error(f"Worker logic error: {e}\nHere is the data received from request: \n{data}")
                    res.close()
                    raise Exception(f"Worker logic error: \n{e}")

            else:
                logger.error("Zero row returned. Maybe wrong station id?")
                raise Exception("Zero row returned. Maybe wrong station id?")
            
            result.close()
            client.close()
        
        except Exception as e:
            logger.error(f"SERVICE WORKER ERROR: {e}")
            raise Exception(f"SERVICE WORKER ERROR: {e}")
    else:
        logger.error("Pass the script path as argument, you will.")
        raise Exception("Pass the script path as argument, you will.")