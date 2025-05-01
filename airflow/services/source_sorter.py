# # # # # # # # # # 
# Universal Sorter #
# # # # # # # # # #
#  
# snames are delimited by '-'. 
# The first one is the source category, and the second one--the sub channel

import json
from common import get_client, get_logger
from deenum import IngestQueries

logger = get_logger("SOURCE-01")

if __name__ == '__main__':
    try:
        client = get_client()
        result = client.query(IngestQueries.GET_SOURCES.value)

        if result.row_count > 0:
            payload = []
            result_set = result.result_set
            
            for row in result_set:
                payload.append({"category": row[0], "source": row[1], "sname": row[2]})

            # is this inside the service worker?? i'm confused
            with open('/workspace/sources.json', 'w') as f:
                json.dump(payload, f, indent=4)

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

        