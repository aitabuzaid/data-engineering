import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song-data/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id",
                             "title",
                             "artist_id",
                             "year",
                             "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
               .partitionBy(['year', 'artist_id'])\
               .mode('overwrite')\
               .parquet('song/song.parquet')
    print("Saved the songs table as parquet files")

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id",
                                   "artist_name AS name",
                                   "artist_location AS location",
                                   "artist_latitude AS latitude",
                                   "artist_longitude AS longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write\
                 .mode('overwrite')\
                 .parquet('artist/artist.parquet')
    print("Saved the artists table as parquet files")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+'log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.filter((col('userId').isNotNull()) 
                          & (col('userId') != ''))\
                    .orderBy(['ts'], ascending = False)\
                    .selectExpr(["userId as user_id",
                                 "firstName AS first_name",
                                 "lastName AS last_name",
                                 "gender",
                                 "level"])\
                    .dropDuplicates(subset=['user_id'])\
                    .withColumn("user_id", col("user_id").cast(IntegerType()))\
                    .orderBy(['user_id'])
    
    # write users table to parquet files
    users_table.write\
               .mode('overwrite')\
               .parquet('user/user.parquet')
    print("Saved the users table as parquet files")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # read in song data to use for songplays table
    song_data = input_data+'song-data/song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, 
                 (df.song == song_df.title) & (df.artist == song_df.artist_name),
                 'left').selectExpr(["ts AS start_time",
                                     "userId AS user_id",
                                     "level",
                                     "song_id",
                                     "artist_id",
                                     "sessionId AS session_id",
                                     "location",
                                     "userAgent AS user_agent"])
    
    songplays_table = songplays_table\
        .withColumn("start_time", (F.to_timestamp(songplays_table.start_time/1000)))\
        .withColumn("songplay_id", F.monotonically_increasing_id())
    songplays_table = songplays_table\
        .withColumn("year", F.year(songplays_table.start_time))\
        .withColumn("month", F.month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month'])\
                  .mode('overwrite')\
                  .parquet('songplays/songplays.parquet')
    print("Saved the songplays table as parquet files")
    
    # extract columns to create time table
    time_table = songplays_table.select(songplays_table.start_time)\
        .withColumn("hour", F.hour(songplays_table.start_time))\
        .withColumn("day", F.dayofmonth(songplays_table.start_time))\
        .withColumn("week", F.weekofyear(songplays_table.start_time))\
        .withColumn("month", F.month(songplays_table.start_time))\
        .withColumn("year", F.year(songplays_table.start_time))\
        .withColumn("weekday", F.dayofweek(songplays_table.start_time))
    
    # write time table to parquet files partitioned by year and month
    time_table.write\
              .partitionBy(['year', 'month'])\
              .mode('overwrite')\
              .parquet('time/time.parquet')
    print("Saved the time table as parquet files")


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
