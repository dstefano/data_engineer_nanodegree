from datetime import datetime
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from datetime import datetime
from pyspark.sql import types as T
from pyspark.sql import functions as F


# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data"
    
    # read song data file
    df_song = spark.read.json(song_data + "/*/*/*")

    # extract columns to create songs table
    songs_table = df_song[['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet("songs.parquet")

    # extract columns to create artists table
    artists_table = df_song[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    
    # write artists table to parquet files
    artists_table.write.parquet("artists.parquet")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    # note: song plays needs all 'NextSong' results
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 
    
    # write users table to parquet files
    users_table.write.parquet("user.parquet")

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # create table to extract columns    
    df.createOrReplaceTempView("log_staging_table")

    # create columns for time table
    time_table = spark.sql('''
        SELECT start_time,
        EXTRACT(hour from start_time) as hour,
        EXTRACT(day from start_time) as day,
        EXTRACT(week from start_time) as week,
        EXTRACT(month from start_time) as month,
        EXTRACT(year from start_time) as year,
        DAYOFWEEK(start_time) as weekday
        
        from log_staging_table
    ''').collect()
    
    # write time table to parquet files partitioned by year and month
    time_table_dataframe = spark.createDataFrame(time_table)
    time_table_dataframe.write.partitionBy("year","month").parquet("time.parquet")

    # read in song data to use for songplays table
    song_data = input_data + "song_data"
    song_df = spark.read.json(song_data + "/*/*/*")
    song_df.createOrReplaceTempView("songs_staging_table")

    # extract columns from joined song and log datasets to create songplays table 
    temp_table = spark.sql('''
        SELECT 
        
            a.start_time, a.userId, a.level, b.song_id,
            b.artist_id, a.sessionId, a.location, a.userAgent,
            EXTRACT(month from a.start_time) as month,
            EXTRACT(year from a.start_time) as year
        from log_staging_table a
        inner join songs_staging_table b on a.song = b.title
    ''').collect()

    songplays_table = spark.createDataFrame(temp_table)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet("songplays.parquet")


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
