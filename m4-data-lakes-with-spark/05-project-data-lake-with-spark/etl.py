import os
import configparser

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# aws access key and secret

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Get/Create a spark session. 
    Depending on the size of the data beign processed you might need to change some of the configuration.
    
    Returns:
        Spark session.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
        # .config("spark.driver.memory", "15g") \
    
    return spark


def process_song_data(spark, input_data, output_data):
    """Process the song-data dataset from input_data, creates songs and artists tables and save them into output_data.
    
    Args:
        spark: Spark session.
        input_data: S3 path to the song-data dataset.
        output_data: S3 path to save the processed data.
    """
    
    # get filepath to song data file
    song_data = 'song-data/*/*/*'
    
    # read song data file
    df = spark.read.json(input_data + song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """Process the log-data dataset from input_data, creates users, time and songplays tables and save them into output_data.
    
    Args:
        spark: Spark session.
        input_data: S3 path to the log-data dataset.
        output_data: S3 path to save the processed data.
    """
        
    # get filepath to log data file
    log_data = 'log-data/*/*'

    # read log data file
    df = spark.read.json(input_data + log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(
        'userId as user_id', 
        'firstName as first_name', 
        'lastName as last_name',
        'gender',
        'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1e3)
    df = df.withColumn('ts_timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn('ts_datetime', get_datetime(col('ts_timestamp')))
    
    # create dayofweek function
    dayofweek = udf(lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%w'))

    # extract columns to create time table
    time_table = df.selectExpr(
        'ts as start_time',
        'hour(ts_datetime) as hour',
        'dayofmonth(ts_datetime) as day',
        'weekofyear(ts_datetime) as week',
        'month(ts_datetime) as month',
        'year(ts_datetime) as year',
        'dayofweek(ts_datetime) as weekday').dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration)).selectExpr(
        'ts as start_time',
        'userId as user_id',
        'level',
        'song_id',
        'artist_id',
        'sessionId as session_id',
        'location',
        'userAgent as user_agent',
        'year(ts_datetime) as year',
        'month(ts_datetime) as month'
    )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays')

    
def main():
    """Processes song and log data from input_data and saves the result into output_data."""
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://[your-final-S3-bucket]/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == "__main__":
    main()
