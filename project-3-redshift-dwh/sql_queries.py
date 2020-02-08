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
    gender          VARCHAR(1),
    item_in_session INT2,
    last_name       VARCHAR,
    length          NUMERIC,
    level           VARCHAR(4),
    location        VARCHAR,
    method          VARCHAR(4),
    page            VARCHAR,
    registration    BIGINT,
    session_id      INTEGER,
    song            VARCHAR,
    status          INT2,
    time_stamp      BIGINT      NOT NULL,
    user_agent      VARCHAR,
    user_id         INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR(18)  NOT NULL,
    artist_latitude  NUMERIC,
    artist_longitude NUMERIC,
    artist_location  VARCHAR,
    artist_name      VARCHAR      NOT NULL,
    song_id          VARCHAR(18)  NOT NULL     PRIMARY KEY,
    title            VARCHAR      NOT NULL,
    duration         NUMERIC,
    year             INT2);
""")

songplay_table_create= ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INTEGER     NOT NULL       PRIMARY KEY    IDENTITY(0,1),
    start_time      TIMESTAMP   NOT NULL       SORTKEY,
    user_id         INTEGER     NOT NULL       DISTKEY,
    level           VARCHAR(4)  NOT NULL,
    song_id         VARCHAR(18) NOT NULL,
    artist_id       VARCHAR(18) NOT NULL,
    session_id      INTEGER     NOT NULL,
    location        VARCHAR,
    user_agent      VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER    NOT NULL PRIMARY KEY SORTKEY DISTKEY,
    first_name      VARCHAR    NOT NULL,
    last_name       VARCHAR    NOT NULL,
    gender          VARCHAR(1) NOT NULL,
    level           VARCHAR(4) NOT NULL);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id         VARCHAR(18) NOT NULL PRIMARY KEY SORTKEY,
    title           VARCHAR     NOT NULL,
    artist_id       VARCHAR(18) NOT NULL,
    year            INT2        NOT NULL,
    duration        NUMERIC NOT NULL)
    DISTSTYLE ALL;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR(18) NOT NULL PRIMARY KEY SORTKEY,
    name            VARCHAR     NOT NULL,
    location        VARCHAR,
    latitude        NUMERIC,
    longitude       NUMERIC)
    DISTSTYLE ALL;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP  NOT NULL PRIMARY KEY SORTKEY,
    hour        INT2       NOT NULL,
    day         INT2       NOT NULL,
    week        INT2       NOT NULL,
    month       INT2       NOT NULL,
    year        INT2       NOT NULL,
    weekday     INT2       NOT NULL);
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
SELECT timestamp 'epoch' + e.time_stamp /1000 * interval '1 second' AS start_time,
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
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
       user_id,
       first_name,
       last_name,
       gender,
       level
FROM   staging_events
WHERE  user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
       song_id,
       title,
       artist_id,
       year,
       duration
FROM   staging_songs
WHERE  song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT 
       artist_id,
       artist_name      AS name,
       artist_location  AS location,
       artist_latitude  AS latitude,
       artist_longitude AS longitude
FROM   staging_songs
WHERE  artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
       timestamp 'epoch' + time_stamp /1000 * interval '1 second' AS start_time,
       EXTRACT(hour    FROM start_time)     AS hour,
       EXTRACT(day     FROM start_time)     AS day,
       EXTRACT(week    FROM start_time)     AS week,
       EXTRACT(month   FROM start_time)     AS month,
       EXTRACT(year    FROM start_time)     AS year,
       EXTRACT(weekday FROM start_time)     AS weekday
FROM   staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]