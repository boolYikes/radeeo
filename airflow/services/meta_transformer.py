# Should I have gone the .sql path? 🤔 Which is more readable?
import json
from common import get_client, get_logger
from deenum import T1Queries

logger = get_logger("TRANSFORMER-01")

if __name__ == "__main__":
    # Tasks
    # Manual init structured tables
    # new structured - old structured = fresh rows -> append to json
    # new structured replaces old structured -> full refresh
    # everyday, no catchup
    try:
        client = get_client()

        # A fixed-point date boundary for idempotency (cuz the final rows for each source can have varying dates)
        date_bound = client.query(T1Queries.DATE_BOUND.value).result_set[0][0].strftime("%Y-%m-%d")

        # Copy old table to temp table -> ENUM these
        # Order of drop-copying : plays -> songs -> albums -> artists -> sources cuz of the relations
        # They don't cascade but just as a precaution
        # temp_ is the NEW table.
        for table_name in ["fact_plays", "dim_songs", "dim_albums", "dim_artists", "dim_sources", "music_raw"]:
            client.command(T1Queries.DROP(f"temp_{table_name}"))
            client.command(T1Queries.CREATETEMP(table_name))

            # This one is of a different origin
            predicate = f"WHERE DATE(pdate) < '{date_bound}'" if table_name != "dim_sources" else ""

            # Transformation: 
            new_table = f"{T1Queries.SILVER.value[table_name]} {predicate}"
            client.command(T1Queries.INSERTTEMP(table_name, new_table))
            
            # Get new rows from raw for the producer buffer
            if table_name == "music_raw":
                new_rows = client.query(T1Queries.GETNEWROWS(table_name))
                # Append to JSONL buffer, with columns
                cols = new_rows.column_names
                rows = new_rows.result_rows
                with open('/workspace/pressure_cooker.jsonl', 'a', encoding='utf-8') as f:
                    for row in rows:
                        new_line = json.dumps(dict(zip(cols, row)), ensure_ascii=False)
                        f.write(new_line + '\n')
                new_rows.close()
                # Do not refresh raw table
                continue

            # Otherwise, do a full-refresh
            client.command(T1Queries.DROP(table_name))
            client.command(T1Queries.RENAMETEMP(table_name))

        client.close()
    
    except Exception as e:
        client.close()
        logger.error(f"SERVICE WORKER ERROR: {e}")
        raise Exception(f"SERVICE WORKER ERROR: {e}")