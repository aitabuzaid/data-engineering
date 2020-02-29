from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


# Configparser is not needed in Amazon EMR as the script is running
# directly in the cluster

#import configparser
#config = configparser.ConfigParser()
#config.read('dl.cfg')
#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    A function that creates a spark session or returns
    the session if it is already existing
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    A function that processes song data by loading it from s3 input folder,
    processes the data with Spark, and then loads it back to s3 output
    folder in the form of parquet files. The song data is used to create
    two distinct tables: songs and artists.
    
    Keyword arguments:
    spark       -- The created spark session
    input_data  -- The s3 folder path that hosts the songs' data in JSON format
    output_data -- The s3 folder path to save the results of the ETL process in
                   partitioned parquet files. Two tables are saved: songs and artists.
    """
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
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
               .parquet(output_data+'song/song.parquet')
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
                 .parquet(output_data+'artist/artist.parquet')
    print("Saved the artists table as parquet files")


def process_log_data(spark, input_data, output_data):
    """
    A function that processes log data by loading it from s3 input folder,
    processes the data with Spark, and then loads it back to s3 output
    folder in the form of parquet files. The log data is joined with the song data
    to create three distinct tables: users, songplays, and time.
    
    Keyword arguments:
    spark       -- The created spark session
    input_data  -- The s3 folder path that hosts the logs' and songs' data in JSON format
    output_data -- The s3 folder path to save the results of the ETL process in partitioned
                   parquet files. Three tables are saved: users, songplays and time.
    """
    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    
    # No need to filter for page=NextSong now, this is needed to capture the most
    # up-to-date level status (paid or free)
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
               .parquet(output_data+'user/user.parquet')
    print("Saved the users table as parquet files")
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    
    # read in song data to use for songplays table
    song_data = input_data+'song_data/*/*/*/*.json'
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
    
    # convert the start_time bigint column into timestamps
    # create a unique id for songplays
    songplays_table = songplays_table\
        .withColumn("start_time", (F.to_timestamp(songplays_table.start_time/1000)))\
        .withColumn("songplay_id", F.monotonically_increasing_id())
    # Create year and month columns in order to save paritioned parquet files. 
    songplays_table = songplays_table\
        .withColumn("year", F.year(songplays_table.start_time))\
        .withColumn("month", F.month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month'])\
                  .mode('overwrite')\
                  .parquet(output_data+'songplays/songplays.parquet')
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
              .parquet(output_data+'time/time.parquet')
    print("Saved the time table as parquet files")


def main():
    """
    The main subroutine that creates the spark session, and then
    processes song and log data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://spark-ait/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
