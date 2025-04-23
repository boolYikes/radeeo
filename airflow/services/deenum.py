from enum import Enum

class Queries(Enum):
    DATABASE = "radeeo"

    # These are eyes-only and not directly used in scripts.
    CREATE_DIM_SOURCES = """
        CREATE TABLE IF NOT EXISTS radeeo.dim_sources
        (
            source_name String,
            added_on Date,
            url String,
            params String,
            headers String,
            category String
        )
        ENGINE = MergeTree
        ORDER BY (source_name)
    """
    CREATE_DIM_SONGS = """
        CREATE TABLE IF NOT EXISTS radeeo.dim_songs
        (
            song_title String,
            album_title String,
            artist_name String,
            tags String,
            genre String
        )
        ENGINE = MergeTree
        ORDER BY (song_title)
    """
    CREATE_FACT_PLAYS = """
        CREATE TABLE IF NOT EXISTS radeeo.fact_plays
        (
            song_title String,
            play_date Datetime,
            audience Integer,
            source_name String,
            stream String,
        )
        ENGINE = MergeTree
        ORDER BY (song_title)
    """
    CREATE_DIM_ALBUMS = """
        CREATE TABLE IF NOT EXISTS radeeo.dim_albums
        (
            album_title String,
            artist_name String,
            cover String
        )
        ENGINE = MergeTree
        ORDER BY (album_title)
    """
    CREATE_DIM_ARTISTS = """
        CREATE TABLE IF NOT EXISTS radeeo.dim_albums
        (
            artist_name String
        )
        ENGINE = MergeTree
        ORDER BY (artist_name)
    """
    
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
            """
    }

    # Date boundary selector for idempotency
    DATE_BOUND = f"SELECT MAX(date(pdate)) FROM {DATABASE}.music_raw"

    # Simple queries
    def DROP(table_name: str) -> str:
        return f"DROP TABLE IF EXISTS {Queries.DATABASE}.{table_name}"
    
    def CREATETEMP(table_name: str) -> str:
        return f"CREATE TABLE {Queries.DATABASE}.temp_{table_name} AS {Queries.DATABASE}.{table_name}"
    
    def INSERTTEMP(table_name: str, origin_table: str) -> str:
        """Input a subquery for [origin_table]."""
        return f"INSERT INTO {Queries.DATABASE}.temp_{table_name} {origin_table}"
    
    # This is efficient... right?
    def GETNEWROWS(table_name: str) -> str:
        """Arguments should be in table expression!"""
        return f"""
        SELECT *, cityHash64(*) AS hashkey
        FROM 
            (
            SELECT DISTINCT(UPPER(TITLE)) AS song_title, album AS album_title, artist AS artist_name, cover, tag, genre 
            FROM {Queries.DATABASE}.temp_{table_name}
            )
        WHERE cityHash64(*)
        NOT IN
            (
            SELECT cityHash64(*) 
            FROM 
                (
                SELECT DISTINCT(UPPER(TITLE)) AS song_title, album AS album_title, artist AS artist_name, cover, tag, genre 
                FROM {Queries.DATABASE}.{table_name}
                )
            )
        """
    def RENAMETEMP(table_name: str) -> str:
        return f"RENAME TABLE temp_{table_name} TO {table_name}"