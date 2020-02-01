import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
    
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

ARN                    = config.get("IAM_ROLE", "ARN")
    
LOG_DATA               = config.get("S3", "LOG_DATA")
LOG_JSONPATH           = config.get("S3", "LOG_JSONPATH")
SONG_DATA              = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs'
songplay_table_drop = 'DROP TABLE IF EXISTS songplays'
user_table_drop = 'DROP TABLE IF EXISTS users'
song_table_drop = 'DROP TABLE IF EXISTS songs'
artist_table_drop = 'DROP TABLE IF EXISTS artists'
time_table_drop = 'DROP TABLE IF EXISTS time'

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER,
    last_name       VARCHAR,
    length          NUMERIC,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    time_stamp      BIGINT,
    user_agent      VARCHAR,
    user_id         INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  NUMERIC,
    artist_longitude NUMERIC,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         NUMERIC,
    year             INTEGER);
""")

songplay_table_create= ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INTEGER IDENTITY(0,1),
    start_time      BIGINT,
    user_id         INTEGER,
    level           VARCHAR,
    song_id         VARCHAR,
    artist_id       VARCHAR,
    session_id      INTEGER,
    location        VARCHAR,
    user_agent      VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id         VARCHAR,
    title           VARCHAR,
    artist_id       VARCHAR,
    year            INTEGER,
    duration        NUMERIC);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR,
    name            VARCHAR,
    location        VARCHAR,
    latitude        NUMERIC,
    longitude       NUMERIC);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time      DATETIME,
    hour            INTEGER,
    day             INTEGER,
    week            INTEGER,
    month           INTEGER,
    year            INTEGER,
    weekday         INTEGER) 
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    iam_role {}
    COMPUPDATE OFF REGION 'us-west-2'
    FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    iam_role {}
    COMPUPDATE OFF REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id,
                       artist_id, session_id, location, user_agent)
SELECT e.time_stamp,
       e.user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.session_id,
       e.location,
       e.user_agent
FROM   staging_events e
JOIN   staging_songs  s ON (e.artist = s.artist_name AND e.song = s.title)
WHERE  e.page = 'NextSong'
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]