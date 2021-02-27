import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear


def create_spark_session(step_app_name):
    """
    Creates the Spark Session
    :return spark:
    """
    spark = SparkSession \
        .builder \
        .appName(step_app_name) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to load source data and process the data. In the below function
    we are processing the 'song_data' and creating some dimension tables:
    songs and artists in parquet format on S3

    :param spark: SparkSession object
    :param input_data: Source data (song_data)
    :param output_data: Data destination
    :return: None
    """
    # load data into dataframe
    song_data = input_data + "song_data/*/*/*"
    df = spark.read.json(song_data)

    # extract columns to create songs_table
    songs_table = (
        df.select(
            'song_id', 'title', 'artist_id', 'year', 'duration'
        ).distinct()
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy(
        "year", "artist_id"
    ).parquet(output_data + "/songs")

    # extract columns to create artists table
    artists_table = (
        df.select(
            'artist_id',
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('latitude'),
            col('artist_longitude').alias('longitude'),
        ).distinct()
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + '/artists')


def process_log_data(spark, input_data, output_data):
    """
    Function to load source data and process the data. In the below function
    we are processing the 'log_data' and creating our Fact table:
    songplays and also dimension tables: time and users in parquet format.

    :param spark: SparkSession object
    :param input_data: Source data (log_data)
    :param output_data: Data destination
    :return: None
    """
    # load data into dataframe
    log_data = input_data + "log_data/*/*"
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users_table
    users_table = (
        df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + '/users')

    # create timestamp column from original timestamp column
    df = df.withColumn(
        "ts_timestamp",
        F.to_timestamp(F.from_unixtime(
            (col("ts") / 1000), 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp")
    )

    def get_weekday(date):
        import datetime
        import calendar
        date = date.strftime("%m-%d-%Y")  # , %H:%M:%S
        month, day, year = (int(x) for x in date.split('-'))
        weekday = datetime.date(year, month, day)
        return calendar.day_name[weekday.weekday()]

    udf_week_day = udf(get_weekday, T.StringType())

    # extract columns to create time table
    time_table = (
        df.withColumn("hour", hour(col("ts_timestamp")))
        .withColumn("day", dayofmonth(col("ts_timestamp")))
        .withColumn("week", weekofyear(col("ts_timestamp")))
        .withColumn("month", month(col("ts_timestamp")))
        .withColumn("year", year(col("ts_timestamp")))
        .withColumn("weekday", udf_week_day(col("ts_timestamp")))
        .select(
            col("ts_timestamp").alias("start_time"),
            col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            col("weekday")
        )
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy(
        "year", "month"
    ).parquet(output_data + "/time")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    songs_logs = df.join(songs_df, (df.song == songs_df.title))

    # extract columns from joined song and log datasets
    # to create songplays table
    artists_df = spark.read.parquet(os.path.join(
        output_data, "artists")
    )
    artists_songs_logs = songs_logs.alias('a').join(
        artists_df.alias('t'), (songs_logs.artist == artists_df.name) |
                               (songs_logs.location == artists_df.location),
        'left'
    )
    songplays = artists_songs_logs.join(
        time_table, artists_songs_logs.ts_timestamp == time_table.start_time,
        'left'
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('a.location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
    ).distinct().repartition("year", "month")

    songplays_table.write.mode("overwrite").partitionBy(
        "year", "month"
    ).parquet(output_data + '/songplays')


def main(root_bucket, input_data, step_app_name):
    """
    Function to run all of the functions to populate the datalake

    :param root_bucket: Your datalake bucket
    :param input_data: Source data
    :param step_app_name: Spark application and step name
    :return: None
    """
    spark = create_spark_session(step_app_name)
    input_data = input_data
    output_data = f'{root_bucket}/tables/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-rb', '--root_bucket', required=True)
    parser.add_argument('-id', '--input_data', required=True)
    parser.add_argument('-n', '--step_app_name', required=True)
    parsed = parser.parse_args()
    main(parsed.root_bucket, parsed.input_data, parsed.step_app_name)
