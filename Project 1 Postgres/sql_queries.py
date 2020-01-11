# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (time_stamp timestamp,
                                                                  user_id int,
                                                                  level varchar,
                                                                  song_id varchar,
                                                                  artist_id varchar,
                                                                  session_id int,
                                                                  location varchar,
                                                                  user_agent varchar);                                                                  
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int,
                                                          first_name varchar,
                                                          last_name varchar, 
                                                          gender varchar,
                                                          level varchar,
                                                          UNIQUE(user_id));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar,
                                                          title varchar,
                                                          artist_id varchar, 
                                                          year int,
                                                          duration float,
                                                          UNIQUE(song_id));
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar,
                                                              artist_name varchar,
                                                              artist_location varchar, 
                                                              artist_latitude float,
                                                              artist_longitude float,
                                                              UNIQUE(artist_id));
""")
['Time Stamp', 'Hour', 'Day', 'Week of Year', 'Month', 'Year', 'Weekday']
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (time_stamp timestamp,
                                                         hour int,
                                                         day int,
                                                         week_of_year int,
                                                         month int,
                                                         year int,
                                                         weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (time_stamp, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                          VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")


time_table_insert = ("""INSERT INTO time (time_stamp, hour, day, week_of_year, month, year, weekday)
                        VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id FROM
                         songs JOIN artists ON songs.artist_id = artists.artist_id
                  WHERE (songs.title = %s AND artists.artist_name = %s AND songs.duration = %s)
""")

# QUERY LISTS
#create_table_queries = [time_table_create, song_table_create, artist_table_create, user_table_create]
#drop_table_queries = [time_table_drop, song_table_drop, artist_table_drop, user_table_drop]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]