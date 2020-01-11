import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """A function to process the song files and create the songs table,
     and the artists table 
    
    Keyword arguments:
    cur -- a cursor to allow running sql commands through the connection
    filepath -- the actual path to the song file that will be processed
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 
                         'year', 'duration']].values[0])
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print('Error: Could not insert data into the songs table')
        print(e)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name',
                           'artist_location', 'artist_latitude',
                           'artist_longitude']].values[0])
    
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print('Error: Could not insert data into the artists table')
        print(e)


def process_log_file(cur, filepath):
    """A function to process the log files and create the time table,
    the user table, and the songplay table 
    
    Keyword arguments:
    cur -- a cursor to allow running sql commands through the connection
    filepath -- the actual path to the log file that will be processed
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t.dt.to_pydatetime(), t.dt.hour,
                 t.dt.day, t.dt.weekofyear, t.dt.month,
                 t.dt.year, t.dt.weekday]
    column_labels = ['Time Stamp', 'Hour',
                     'Day', 'Week of Year', 'Month',
                     'Year', 'Weekday']
    time_df = pd.DataFrame(time_data, column_labels).T

    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    except psycopg2.Error as e:
        print('Error: Could not insert data into the time table')
        print(e)
        
    # load user table
    user_df = df[['userId','firstName','lastName',
                  'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row.values)

        
    # insert songplay records
    df['timestamp2'] = pd.to_datetime(df['ts'],
                                      unit='ms').dt.to_pydatetime()
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except psycopg2.Error as e:
            print('Error: Could not perform select on songs/artists tables')
            print(e)
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.timestamp2, row.userId, row.level,
                         songid, artistid, row.sessionId,
                         row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print('Error: Could not insert data into songplay table')
            print(e)


def process_data(cur, conn, filepath, func):
    """A generic function that processes the JSON files
    
    Keyword arguments:
    cur -- a cursor to allow running sql commands through the connection
    conn -- a connection to the Postgres database
    filepath -- the file path to the root directory
    func -- the function that will do the actual processing of the log and
            the song files
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))



def main():
    """Establishes a connection to the sparkify database and processes
    the song and log data files. It extracts the JSON files, transforms
    the data, and finally loads the data into the various tables in the
    database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()