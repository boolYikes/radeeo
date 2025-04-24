from enum import Enum

class T1Queries(Enum):
    DATABASE = "radeeo"

    # Normalizers for the second layer transformation
    # Combine these queries with except and temp tables to get new rows
    SILVER = {
        "fact_plays": f"""
            SELECT title AS song_title, pdate AS play_date, aud AS audience, source AS source_name, stream 
            FROM {DATABASE}.music_raw""", 
        "dim_songs":f"""
            SELECT DISTINCT(UPPER(title)) AS song_title, album AS album_title, artist AS artist_name, tags, genre
            FROM {DATABASE}.music_raw
            """, 
        "dim_albums":f"""
            SELECT DISTINCT(UPPER(album)) AS album, artist, cover 
            FROM {DATABASE}.music_raw
            """, 
        "dim_artists":f"""
            SELECT DISTINCT(UPPER(artist)) AS artist_name
            FROM {DATABASE}.music_raw
            """, 
        "dim_sources":f"""
            SELECT sname AS source_name, added_on, url, params, headers, category
            FROM {DATABASE}.source_meta
            """,
        "music_raw":f"""
            SELECT DISTINCT(UPPER(TITLE)) AS song_title, album AS album_title, artist AS artist_name, cover, tag, genre
            FROM {DATABASE}.music_raw
            """
    }

    # Date boundary selector for idempotency
    DATE_BOUND = f"SELECT MAX(date(pdate)) FROM {DATABASE}.music_raw"

    # Simple queries
    def DROP(table_name: str) -> str:
        return f"DROP TABLE IF EXISTS {T1Queries.DATABASE}.{table_name}"
    
    def CREATETEMP(table_name: str) -> str:
        return f"CREATE TABLE {T1Queries.DATABASE}.temp_{table_name} AS {T1Queries.DATABASE}.{table_name}"
    
    def INSERTTEMP(table_name: str, origin_table: str) -> str:
        """Input a subquery for [origin_table]."""
        return f"INSERT INTO {T1Queries.DATABASE}.temp_{table_name} {origin_table}"
    
    # This is efficient... right?
    def GETNEWROWS(table_name: str) -> str:
        """Arguments should be in table expression!"""
        return f"""
        SELECT *, cityHash64(*) AS hashkey
        FROM 
            (
            SELECT DISTINCT(UPPER(TITLE)) AS song_title, album AS album_title, artist AS artist_name, cover, tag, genre 
            FROM {T1Queries.DATABASE}.temp_{table_name}
            )
        WHERE cityHash64(*)
        NOT IN
            (
            SELECT cityHash64(*) 
            FROM 
                (
                SELECT DISTINCT(UPPER(TITLE)) AS song_title, album AS album_title, artist AS artist_name, cover, tag, genre 
                FROM {T1Queries.DATABASE}.{table_name}
                )
            )
        """
    def RENAMETEMP(table_name: str) -> str:
        return f"RENAME TABLE temp_{table_name} TO {table_name}"
    
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