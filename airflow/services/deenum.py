from enum import Enum

class T1Queries(Enum):

    # Normalizers for the second layer transformation
    # Combine these queries with except and temp tables to get new rows
    SILVER = {
        "fact_plays": f"""
            SELECT title AS song_title, pdate AS play_date, aud AS audience, source AS source_name, stream 
            FROM radeeo.music_raw""", 
        "dim_songs":f"""
            SELECT DISTINCT(UPPER(title)) AS song_title, album AS album_title, artist AS artist_name, tag, genre
            FROM radeeo.music_raw
            """, 
        "dim_albums":f"""
            SELECT DISTINCT(UPPER(album)) AS album, artist, cover 
            FROM radeeo.music_raw
            """, 
        "dim_artists":f"""
            SELECT DISTINCT(UPPER(artist)) AS artist_name
            FROM radeeo.music_raw
            """, 
        "dim_sources":f"""
            SELECT sname AS source_name, added_on, url, params, headers, category
            FROM radeeo.source_meta
            """,
        "music_raw":f"""
            SELECT DISTINCT(UPPER(title)) AS song_title, album AS album_title, artist AS artist_name, cover, tag, genre
            FROM radeeo.music_raw
            """
    }

    # Date boundary selector for idempotency
    DATE_BOUND = f"SELECT MAX(date(pdate)) FROM radeeo.music_raw"

    # Simple queries dispatcher
    DROP = "drop"
    CREATETEMP = "create_temp"
    INSERTTEMP = "insert_temp"
    GETNEWROWS = "get_new_rows"
    RENAMETEMP = "rename_temp"
    def do(self, table_name: str, origin_table: str = "") -> str:
        target = 'music_tfd' if table_name == 'music_raw' else table_name
        
        if self == T1Queries.DROP:
            return f"DROP TABLE IF EXISTS radeeo.{target}"
        
        elif self == T1Queries.CREATETEMP:
            return f"CREATE TABLE radeeo.temp_{table_name} AS radeeo.{target}"
        
        # Input a subquery for [origin_table].
        elif self == T1Queries.INSERTTEMP:
            return f"INSERT INTO radeeo.temp_{table_name} {origin_table}"
        
        # Arguments should be in table expression!
        elif self == T1Queries.GETNEWROWS:
            return f"""
                SELECT *, cityHash64(*) AS hashkey
                FROM 
                    (
                    SELECT song_title, album_title, artist_name, cover, tag, genre 
                    FROM radeeo.temp_{table_name}
                    )
                WHERE cityHash64(*)
                NOT IN
                    (
                    SELECT cityHash64(*) 
                    FROM 
                        (
                        SELECT song_title, album_title, artist_name, cover, tag, genre
                        FROM radeeo.{target}
                        )
                    )
            """
        
        elif self == T1Queries.RENAMETEMP:
            return f"RENAME TABLE temp_{table_name} TO {target}"
    
class IngestQueries(Enum):
    CREAT_MUSICRAW = """
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

    # Clickhouse is 1-base-indexed
    GET_SOURCES = """
    SELECT
    category,
    trim(splitByChar('-', sname)[1]) AS source,
    sname
    FROM source_meta
    """
    
    def get_source(source_name: str) -> str:
        return f"""
        SELECT url, params, headers
        FROM source_meta
        WHERE sname = '{source_name}'
        ORDER BY added_on desc
        LIMIT 1
        """
    
    def play_status(title: str) -> str:
        return f"""
        SELECT
        title
        FROM music_raw
        WHERE title = '{title}'
        AND pdate >= now() - INTERVAL 10 MINUTE
        """