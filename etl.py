import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofyear, dayofweek
from sql_helper import staging_songs_schema, staging_events_schema, select_users, select_songs, select_artists, select_time, select_songplays

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"]= config.get("AWS", "AWS_SECRET_ACCESS_KEY")

show=True

def print_show(table):
    if not show:
        return 
    
    print(table.limit(10).show())
    print(table.count())



def create_spark_session():
    """
    Description: This function can be used to get a spark session.

    Arguments:
    None

    Returns:
    None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to process song data to get tables we want and save these tables to files.

    Arguments:
        spark: the spark session object. 
        input_data: input data path 
        out_data: output data path

    Returns:
    None
    """

    staging_songs = spark.read.json(input_data, schema=staging_songs_schema)
    staging_songs.registerTempTable("staging_songs")
    print("create tempTable staging_songs")

  
    songs_table = spark.sql(select_songs)
    print("create songs")
    print_show(songs_table)
    songs_table.registerTempTable("songs")
    songs_table.write.partitionBy(['year', 'artist_id']).parquet('./output/songs/')

    artists_table = spark.sql(select_artists)
    print("create artists")
    artists_table.registerTempTable("artists")
    print_show(artists_table)
    artists_table.write.parquet('./output/artists/')



def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to process log data to get tables we want ant and save these tables to files.

    Arguments:
        spark: the spark session object. 
        input_data: input data path 
        out_data: output data path

    Returns:
    None
    """

    staging_logs = spark.read.json(input_data, schema=staging_events_schema)
    print("create tempTable staging_events")
    staging_logs.registerTempTable("staging_events")
  
    users_table = spark.sql(select_users)
    users_table.registerTempTable("users")
    print("create users", )
    users_table.write.parquet('./output/users/')
    print_show(users_table)

    time_ = spark.sql(select_time)
    time_table = time_.select(
        'start_time',
         hour('start_time').alias('hour'),
         dayofyear('start_time').alias('day'),
         weekofyear('start_time').alias('week'),
         month('start_time').alias('month'),
         year('start_time').alias('year'),
         dayofweek('start_time').alias('weekday')
            )  
    time_table.write.partitionBy(['year', 'month']).parquet('./output/time/')
    print("create time")
    print_show(time_table)
    
    songplays_table = spark.sql(select_songplays)
    songplays_table = songplays_table.withColumn('year', year(songplays_table.start_time) )
    songplays_table = songplays_table.withColumn('month', month(songplays_table.start_time) )

    songplays_table.write.partitionBy(['year', 'month']).parquet('./output/songplays/')
    print("read song_plays")
    print_show(songplays_table)


def main():
    """
    Description: This function can be used to get star schema tables for sparkify bussiness .

    Arguments:
    None

    Returns:
    None
    """
    spark = create_spark_session()
    
    log_data_path = "s3a://udacity-dend/log_data/*/*/*.json"
    song_data_path = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
# keeped for debug in local environment.    
#     log_data_path = "./data/log_data/*.json"
#     song_data_path = "./data/song_data/*/*/*/*.json"
    
    output_data = "./output/"
    
    process_song_data(spark, song_data_path, output_data)    
    process_log_data(spark, log_data_path, output_data)


if __name__ == "__main__":
    main()
