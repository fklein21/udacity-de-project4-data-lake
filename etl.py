from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, concat_ws
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Processes the song data.

    This function reads the song data (in json format) from 
    an input S3 bucket, processes it and writes the processed data
    into an output S3 bucket.

    Two parquet files will be written to the output directory containing 
    the dimensional data about the songs and the artists.

    Args:
        spark (SparkSession) : The spark session that executes the computation.
        input_data (str) : The path to the directory (in a S3 bucket) containing 
                           the input files.
        output_data (str) : The path to the directory (in a S3 bucket) where
                            the processed data should be written.
    '''
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data).distinct()

    # extract columns to create songs table
    songs_table = (
        df
        .select(
            "song_id",
            "title",
            "artist_id",
            "year",
            "duration",
        )
        .repartition("year", "artist_id")
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = (
        df
        .select(
            "artist_id",
            col("artist_name").alias("name"),
            col("artist_location").alias("location"),
            col("artist_latitude").alias("latitude"),
            col("artist_longitude").alias("longitude"),
        )
        .distinct()
        .repartition("artist_id")
    )
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    '''Processes the log data.

    This function reads the log data (in json format) from 
    an input S3 bucket, processes it and writes the processed data
    into an output S3 bucket.

    Three parquet files will be written to the output directory containing 
        - the dimensional data about the users and the timestamps, and
        - the fact table of all song play events.
        
    Args:
        spark (SparkSession) : The spark session that executes the computation.
        input_data (str) : The path to the directory (in a S3 bucket) containing 
                           the input files.
        output_data (str) : The path to the directory (in a S3 bucket) where
                            the processed data should be written.
    '''
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table
    df = (
        df
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
    )
    users_table = (
        df
        .select(
            "user_id",
            "first_name",
            "last_name",
            "gender",
            "level"
        )
        .distinct()
        .repartition("user_id")
    )

    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet")

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", to_timestamp(col("ts")/1000))
    
    # # extract columns to create time table
    time_table = (
        df
        .select("start_time")
        .distinct()
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", (dayofweek("start_time") - 1) % 7 + 1)
        .repartition("year", month(col("start_time")))
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
        df
        .join(
            song_df,
            [
                df["song"] == song_df["title"],
                df["length"] == song_df["duration"]
            ],
            "left"
        )
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("userAgent", "user_agent")
        .withColumn("songplay_id", concat_ws("-", 
                                             col("session_id"),
                                             col("ts").cast("string")))
        .select(
            "songplay_id",
            "start_time",
            "user_id",
            "level",
            "song_id",
            "artist_id",
            "session_id",
            "location",
            "user_agent"
        )
        .repartition(year(col("start_time")), month(col("start_time")))
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-project-output-9/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
