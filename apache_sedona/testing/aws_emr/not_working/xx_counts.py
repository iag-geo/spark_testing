
# script to get key stats fom AIS shipping data

import logging
import os
import sys

from datetime import datetime

from pyspark.sql import functions as f, types as t  # need to install pyspark package
from pyspark.sql import SparkSession, Window

# set logger
log_file = os.path.abspath(__file__).replace(".py", ".log")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p")

from geospark.register import upload_jars, GeoSparkRegistrator  # need to install geospark package
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator

s3_bucket = "minus34.com"

# file paths
# input_path = "raw/iag-mo-000000000112.csv"
point_input_path = "point"
final_point_input_path = "final_point"
trip_input_path = "trip"
stop_input_path = "stop"
output_path = "stats"

# S3 paths - must use the s3:// prefix for S3 files
point_source_s3_path = "s3://{}/{}".format(s3_bucket, point_input_path)
final_point_source_s3_path = "s3://{}/{}".format(s3_bucket, final_point_input_path)
trip_source_s3_path = "s3://{}/{}".format(s3_bucket, trip_input_path)
stop_source_s3_path = "s3://{}/{}".format(s3_bucket, stop_input_path)
target_s3_path = "s3://{}/{}".format(s3_bucket, output_path)


def main():
    start_time = datetime.now()

    # upload Sedona (geospark) JARs
    upload_jars()

    spark = (SparkSession
             .builder
             .appName("query")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.hadoop.fs.s3.fast.upload", "true")
             .config("spark.sql.adaptive.enabled", "true")  # TODO: does this split one ID into 2 or more partitions?
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)
             .getOrCreate()
             )

    # Register Apache Sedona (geospark) UDTs and UDFs
    GeoSparkRegistrator.registerAll(spark)

    sc = spark.sparkContext
    sc.setCheckpointDir("hdfs:///checkpoints")

    logger.info("{} initiated on PySpark {} : {}"
                .format(sc.applicationId, sc.version, datetime.now() - start_time))
    logger.info("\t - Running on Python {}".format(sys.version.replace("\n", " ")))
    start_time = datetime.now()

    # -----------------------------------------------------------------------------------------
    # 1. vehicle point counts with min/max times
    # -----------------------------------------------------------------------------------------

    logger.info("1. Counts per ID")

    df = spark.read.parquet(point_source_s3_path)
    logger.info("\t - {:,} points".format(df.count()))

    count_df = df.groupBy("uid", "src") \
        .agg(f.count("*").alias("point_count"),
             f.min("time_utc").alias("min_time_utc"),
             f.max("time_utc").alias("max_time_utc")
             ) \
        .withColumn("total_hours", f.round((f.col("max_time_utc").cast(t.LongType())
                                    - f.col("min_time_utc").cast(t.LongType())).cast(t.DoubleType()) / 3600.0, 1))

    logger.info("\t - {:,} unique IDs".format(count_df.count()))

    # -----------------------------------------------------------------------------------------
    # add days active
    # -----------------------------------------------------------------------------------------

    days_df = df.groupBy("uid", f.to_date("time_utc").alias("date_utc"))\
        .agg(f.count("*").alias("point_count"))  # only required to convert GroupedData object into a DataFrame

    logger.info("\t - {:,} ID days of data".format(days_df.count()))

    days_df2 = days_df.groupBy("uid") \
        .agg(f.count("*").alias("active_day_count"))
    # days_df2.printSchema()
    # days_df2.show(20, False)

    # add data to counts
    count_df2 = count_df.join(days_df2, "uid", "inner")

    # -----------------------------------------------------------------------------------------
    # add trip counts
    # -----------------------------------------------------------------------------------------

    trip_df = spark.read.parquet(trip_source_s3_path)
    logger.info("\t - {:,} trips".format(trip_df.count()))

    trip_df2 = (trip_df.groupBy("uid")
                .agg(f.count("*").alias("trip_count"),
                     f.sum("point_count").alias("trip_point_count"),
                     f.sum(f.round("distance_km", 3)).alias("trip_distance_km"),
                     f.sum("duration_s").alias("temp_duration_s"),
                     )
                .withColumn("trip_hours", f.round(f.col("temp_duration_s").cast(t.DoubleType()) / 3600.0, 1))
                .withColumn("trip_avg_seconds_per_point", f.round(
                            f.when(f.col("temp_duration_s") > 0, f.col("temp_duration_s") /
                                   f.col("trip_point_count").cast(t.DoubleType()))
                            .otherwise(None), 1))
                .withColumn("trip_avg_speed", f.round(
                            f.when(f.col("trip_distance_km") > 0, f.col("trip_distance_km") /
                                   f.col("trip_hours"))
                            .otherwise(None), 1))
                .drop("temp_duration_s")
                )
    # trip_df2.printSchema()
    # trip_df2.show(10, False)

    logger.info("\t - {:,} IDs with trips".format(trip_df2.count()))

    # add trip data to counts
    count_df3 = count_df2.join(trip_df2, "uid", "left")

    # -----------------------------------------------------------------------------------------
    # add stop counts
    # -----------------------------------------------------------------------------------------

    stop_df = spark.read.parquet(stop_source_s3_path)
    logger.info("\t - {:,} stops".format(stop_df.count()))

    stop_df2 = (stop_df.groupBy("uid")
                .agg(f.count("*").alias("stop_count"),
                     f.sum("point_count").alias("stop_point_count"),
                     f.sum(f.round("distance_km", 3)).alias("stop_distance_km"),
                     f.sum("duration_s").alias("temp_duration_s"),
                     )
                .withColumn("stop_hours", f.round(f.col("temp_duration_s").cast(t.DoubleType()) / 3600.0, 1))
                .withColumn("stop_avg_seconds_per_point", f.round(
        f.when(f.col("temp_duration_s") > 0, f.col("temp_duration_s") /
               f.col("stop_point_count").cast(t.DoubleType()))
            .otherwise(None), 1))
                .withColumn("stop_avg_speed", f.round(
        f.when(f.col("stop_distance_km") > 0, f.col("stop_distance_km") /
               f.col("stop_hours"))
            .otherwise(None), 1))
                .drop("temp_duration_s")
                )
    # stop_df2.printSchema()
    # stop_df2.show(10, False)

    logger.info("\t - {:,} IDs with stops".format(stop_df2.count()))

    # add stop data to counts
    id_count_df = count_df3.join(stop_df2, "uid", "left") \
        .checkpoint()

    # -----------------------------------------------------------------------------------------
    # output counts
    # -----------------------------------------------------------------------------------------

    (id_count_df
        .repartition(1)
        .write
        .option("compression", "gzip")
        .option("header", "true")
        .option("nullValue", None)
        .mode("overwrite")
        .csv(os.path.join(target_s3_path, "counts"))
     )

    # get IDs that have more than a fleeting amount of data
    # useful_id_df = id_count_df.filter()

    # id_count_df.unpersist()
    stop_df2.unpersist()
    count_df3.unpersist()
    trip_df2.unpersist()
    count_df2.unpersist()
    days_df2.unpersist()
    count_df.unpersist()
    df.unpersist()

    # -----------------------------------------------------------------------------------------
    # 2. counts of GPS refresh rates
    # -----------------------------------------------------------------------------------------

    logger.info("2. GPS refresh rates")

    final_point_df = spark.read.parquet(final_point_source_s3_path)
    final_point_df.createOrReplaceTempView("pnt")

    sql = """SELECT src,
                    count(*) as point_count,
                    SUM(CASE WHEN next_interval <= 5 THEN 1 ELSE 0 END) as _05_seconds,
                    SUM(CASE WHEN next_interval <= 10 THEN 1 ELSE 0 END) as _10_seconds,
                    SUM(CASE WHEN next_interval <= 15 THEN 1 ELSE 0 END) as _15_seconds,
                    SUM(CASE WHEN next_interval <= 20 THEN 1 ELSE 0 END) as _20_seconds,
                    SUM(CASE WHEN next_interval <= 30 THEN 1 ELSE 0 END) as _30_seconds,
                    SUM(CASE WHEN next_interval <= 45 THEN 1 ELSE 0 END) as _45_seconds,
                    SUM(CASE WHEN next_interval <= 60 THEN 1 ELSE 0 END) as _60_seconds,
                    SUM(CASE WHEN next_interval <= 120 THEN 1 ELSE 0 END) as _120_seconds,
                    SUM(CASE WHEN next_interval > 120 THEN 1 ELSE 0 END) as _over_120_seconds
             FROM pnt
             GROUP BY src"""
    refresh_rate_df = spark.sql(sql)
    refresh_rate_df.orderBy("src").show(20, False)

    refresh_rate_df.unpersist()
    final_point_df.unpersist()

    # -----------------------------------------------------------------------------------------
    # 3. trips per day
    # -----------------------------------------------------------------------------------------

    logger.info("3. Trips & Stops per day")

    trip_count_df = (trip_df
                     .withColumn("date_local", f.col("start_time_local").cast(t.DateType()))
                     .groupBy("src", f.col("date_local"))
                     .agg(f.count("*").alias("trip_count"),
                          # f.count("distinct uid").alias("trip_id_count_df"),
                          f.sum("point_count").alias("trip_point_count"),
                          f.sum("distance_km").alias("trip_distance_km"),
                          f.sum("duration_s").alias("temp_duration_s"),
                          )
                     .withColumn("trip_hours", f.round(f.col("temp_duration_s").cast(t.DoubleType()) / 3600.0, 1))
                     .withColumn("trip_avg_seconds_per_point",
                                 f.round(f.when(f.col("temp_duration_s") > 0, f.col("temp_duration_s") /
                                                f.col("trip_point_count").cast(t.DoubleType())).otherwise(None), 1))
                     .withColumn("trip_avg_speed",
                                 f.round(f.when(f.col("trip_distance_km") > 0, f.col("trip_distance_km") /
                                 f.col("trip_hours")).otherwise(None), 1))
                     .withColumn("trip_distance_km", f.round(f.col("trip_distance_km"), 3))
                     .drop("temp_duration_s")
                     )

    trip_df.unpersist()

    logger.info("\t - {:,} days of trips".format(trip_count_df.count()))

    stop_count_df = (stop_df
                     .withColumn("date_local", f.col("start_time_local").cast(t.DateType()))
                     .groupBy("src", f.col("date_local"))
                     .agg(f.count("*").alias("stop_count"),
                          # f.count("distinct uid").alias("stop_id_count_df"),
                          f.sum("point_count").alias("stop_point_count"),
                          f.sum("distance_km").alias("stop_distance_km"),
                          f.sum("duration_s").alias("temp_duration_s"),
                          )
                     .withColumn("stop_hours", f.round(f.col("temp_duration_s").cast(t.DoubleType()) / 3600.0, 1))
                     .withColumn("stop_avg_seconds_per_point",
                                 f.round(f.when(f.col("temp_duration_s") > 0, f.col("temp_duration_s") /
                                                f.col("stop_point_count").cast(t.DoubleType())).otherwise(None), 1))
                     .withColumn("stop_avg_speed",
                                 f.round(f.when(f.col("stop_distance_km") > 0, f.col("stop_distance_km") /
                                                f.col("stop_hours")).otherwise(None), 1))
                     .withColumn("stop_distance_km", f.round(f.col("stop_distance_km"), 3))
                     .drop("temp_duration_s")
                     )

    stop_df.unpersist()

    logger.info("\t - {:,} days of stops".format(stop_count_df.count()))

    # combine trip and stop counts
    trip_stop_count = trip_count_df.alias("left").join(stop_count_df.alias("right"), ["src", "date_local"], "full")
    # trip_stop_count.printSchema()
    # trip_stop_count.show(20, False)

    (trip_stop_count
     .repartition(1)
     .write
     .option("compression", "gzip")
     .option("header", "true")
     .option("nullValue", None)
     .mode("overwrite")
     .csv(os.path.join(target_s3_path, "daily_trip_stop_counts"))
     )

    # cleanup
    spark.stop()


if __name__ == "__main__":
    full_start_time = datetime.now()

    # setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # set Spark logging levels
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # setup logger to write to screen as well as writing to log file
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)

    task_name = "Compass IoT Data Processing"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
