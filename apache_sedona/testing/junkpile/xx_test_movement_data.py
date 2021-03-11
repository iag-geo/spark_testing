
import glob
import logging
import os
import psycopg2  # need to install psycopg2 (binary) package
import sys

from datetime import datetime
from itertools import repeat
from multiprocessing import Pool, cpu_count
from psycopg2 import pool

from pyspark.sql import functions as f, types as t  # need to install pyspark package
from pyspark.sql import SparkSession, Window

from sedona.register import upload_jars, SedonaRegistrator  # need to install sedona package
from sedona.utils import KryoSerializer, SedonaKryoRegistrator

# # REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# # set Conda environment vars for PySpark
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"] = "/Users/hugh.saalmans/spark-2.4.6-bin-hadoop2.7"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# os.environ["PYSPARK_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"

# input path for parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data")

# number of CPUs to use in processing (defaults to 2x local CPUs)
num_processors = cpu_count() * 2


# set postgres connection parameters
def get_password(connection_name):
    # get credentials from local passwords file
    passwords_file_path = os.path.join(os.environ["GIT_HOME"], "passwords.ini")

    if os.path.exists(passwords_file_path):
        passwords_file = open(passwords_file_path,'r').read().splitlines()
        passwords_file = [i for i in passwords_file if len(i) != 0]  # remove empty lines
        passwords_file = [i for i in passwords_file if i[0] != "#"]  # remove comment lines

        params = dict()
        for ini in passwords_file:
            params[ini.split()[0].rstrip().lstrip()] = ini.split(':=')[1].rstrip().lstrip()

        return dict(item.split("|") for item in params[connection_name].split(","))


local_pg_settings = get_password("localhost_super")

# get connect string for psycopg2
local_pg_connect_string = "dbname={DB} host={HOST} port={PORT} user={USER} password={PASS}".format(**local_pg_settings)

# create Postgres connection pool
pg_pool = psycopg2.pool.SimpleConnectionPool(1, num_processors, local_pg_connect_string)

# # spatial filter for Australia
# bbox = {
#     "min_x": 112.8,
#     "min_y": -43.9,
#     "max_x": 154.0,
#     "max_y": -9.1
# }

# # spatial filter for southern part of Muswellbrook, NSW
# bbox = {
#     "min_x": 150.865492,
#     "min_y": -32.285090,
#     "max_x": 150.911417,
#     "max_y": -32.255802
# }

# spatial filter for Leichhardt, NSW
bbox = {
    "min_x": 151.128870,
    "min_y": -33.893983,
    "max_x": 151.163727,
    "max_y": -33.875505
}

# file paths
input_file_name = "/Users/hugh.saalmans/tmp/movement_data/deduplicated/"
output_path = "/Users/hugh.saalmans/tmp/movement_data"


def main():
    start_time = datetime.now()

    # upload Sedona (sedona) JARs
    upload_jars()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("query")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
             .config("spark.cores.max", num_processors)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.driver.memory", "12g")
             .getOrCreate()
             )
    #              .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    #              .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    #              .config("spark.sql.autoBroadcastJoinThreshold", -1)
    #              .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    #              .config("spark.driver.maxResultSize", "1g")
    #
    #              .config("spark.executor.cores", 1)
    #              .config("spark.executor.memory", "2g")

    # Register Apache Sedona (geospark) UDTs and UDFs
    SedonaRegistrator.registerAll(spark)

    logger.info("PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))
    logger.info("\t - Running on Python {}".format(sys.version.replace("\n", " ")))
    start_time = datetime.now()

    # # load gzip csv files
    # df = spark.read.csv(input_file_name)
    # # df = spark.read.csv(os.path.join(output_path, "testing"))
    # # df = spark.read.csv(os.path.join(output_path, "sydney"))
    # # df.printSchema()
    # # df.show()
    #
    # # # create small dataset to speed testing up
    # # testing_df = df.filter(f.col("_c0").isin(vehicle_id_list)).cache()
    # # print(testing_df.count())
    # # testing_df.repartition(1).write.option("compression", "gzip") \
    # #     .mode("overwrite") \
    # #     .csv(os.path.join(output_path, "testing"))
    #
    # # fix column types and names - for some unknown reason it's 3-4x faster than enforcing schema on load
    # df2 = (df.withColumnRenamed("_c0", "vehicle_id")
    #        .withColumn("longitude", df["_c1"].cast(t.DoubleType()))
    #        .withColumn("latitude", df["_c2"].cast(t.DoubleType()))
    #        .withColumn("speed", df["_c3"].cast(t.DoubleType()))
    #        .withColumn("bearing", df["_c4"].cast(t.DoubleType()))
    #        .withColumn("time_utc", df["_c5"].cast(t.TimestampType()))
    #        .withColumn("unix_time", df["_c6"].cast(t.IntegerType()))
    #        .withColumn("geom", f.expr("st_point(longitude, latitude)"))
    #        .drop("_c1")
    #        .drop("_c2")
    #        .drop("_c3")
    #        .drop("_c4")
    #        .drop("_c5")
    #        .drop("_c6")
    #        .repartition(f.to_date(f.col("time_utc")))
    #        )
    # # df2.printSchema()
    # # df2.show(10, False)
    #
    # df2.write.option("compression", "gzip") \
    #     .mode("overwrite") \
    #     .parquet(os.path.join(output_path, "step_1_schema_applied"))
    #
    # df.unpersist()
    # df2.unpersist()

    schema_df = spark.read.parquet(os.path.join(output_path, "step_1_schema_applied"))
    schema_df.createOrReplaceTempView("point")

    # # # get counts
    # # sql = """SELECT count(distinct vehicle_id) as unique_id_count,
    # #                 count(*) as point_count
    # #          FROM point"""
    # # area_df = spark.sql(sql)
    # # area_df.show()
    #
    # logger.info("Step 1 : {} points loaded : {}".format(schema_df.count(), datetime.now() - start_time))
    # # start_time = datetime.now()

    # --------------------------
    # output stuff
    # --------------------------

    # get_time_gap_stats(spark)
    export_trip_segments(spark)
    # export_small_area_data(spark)
    # export_single_id_data(spark)
    # export_trip_and_stop_data(spark)

    # --------------------------

    # cleanup
    spark.stop()
    pg_pool.closeall()


def get_time_gap_stats(spark):
    logger.info("Processing time gap stats")
    start_time = datetime.now()

    # get all time gaps
    sql = """SELECT unix_time - lag(unix_time) OVER (PARTITION BY vehicle_id ORDER BY time_utc) as prev_time_gap_s,
                    lead(unix_time) OVER (PARTITION BY vehicle_id ORDER BY time_utc) - unix_time as next_time_gap_s,
                    speed
             FROM point"""
    full_df = spark.sql(sql)
    # full_df.printSchema()
    # full_df.show()

    full_df.createOrReplaceTempView("time_gaps")

    # get stats on time gaps
    sql = """with gaps as (
                 select count(*) as point_count,
                        round(speed, -1) as speed,
                        sum(case when prev_time_gap_s <= 5 and next_time_gap_s <= 5 then 1 else 0 end) as sec_5_both_ways,
                        sum(case when prev_time_gap_s = 0 then 1 else 0 end) as sec_0,
                        sum(case when prev_time_gap_s <= 5 then 1 else 0 end) as sec_5,
                        sum(case when prev_time_gap_s <= 10 then 1 else 0 end) as sec_10,
                        sum(case when prev_time_gap_s <= 15 then 1 else 0 end) as sec_15,
                        sum(case when prev_time_gap_s <= 30 then 1 else 0 end) as sec_30,
                        sum(case when prev_time_gap_s <= 60 then 1 else 0 end) as sec_60,
                        sum(case when prev_time_gap_s <= 120 then 1 else 0 end) as sec_120
                from time_gaps
                group by round(speed, -1)
            )
                select point_count,
                       speed,
                       cast(sec_5_both_ways / point_count * 100.0 as integer) as sec_5_both_ways,
                       cast(sec_0 / point_count * 100.0 as integer) as sec_0,
                       cast(sec_5 / point_count * 100.0 as integer) as sec_5,
                       cast(sec_10 / point_count * 100.0 as integer) as sec_10,
                       cast(sec_15 / point_count * 100.0 as integer) as sec_15,
                       cast(sec_30 / point_count * 100.0 as integer) as sec_30,
                       cast(sec_60 / point_count * 100.0 as integer) as sec_60,
                       cast(sec_120 / point_count * 100.0 as integer) as sec_120
                from gaps
                order by speed"""
    stats_df = spark.sql(sql)
    stats_df.show(50, False)

    logger.info("Got time gap stats : {}".format(datetime.now() - start_time))


def export_trip_segments(spark):
    logger.info("Processing line segments")

    # get linestrings between current and previous points - where vehicle is moving
    sql = """WITH gaps AS (
                 SELECT time_utc,
                        cast(speed as integer) as speed,
                        unix_time - lag(unix_time) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) as prev_time_gap_s,
                        case when lag(geom) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) is not null
                            then st_distance(ST_Transform(geom, 'epsg:4326','epsg:3577'), lag(ST_Transform(geom, 'epsg:4326','epsg:3577')) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc))
                            else null end as prev_distance_m,
                        geom,
                        case when lag(geom) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) is not null
                            then lag(geom) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc)
                            else null end as prev_geom
                 FROM point
                 WHERE speed >= 10.0
             )
             SELECT time_utc,
                    speed,
                    cast(prev_time_gap_s as integer) as prev_time_gap_s,
                    cast(prev_distance_m as integer) as prev_distance_m,
                    concat('SRID=4326;LINESTRING (', ST_X(prev_geom), ' ', ST_Y(prev_geom), ',', ST_X(geom), ' ', ST_Y(geom), ')') as wkt_geom
             FROM gaps
             WHERE prev_geom is not null"""
    segment_df = spark.sql(sql).repartition(f.to_date(f.col("time_utc"))).drop("time_utc")

    # export df to postgres
    export_to_postgres(segment_df, "testing.test_oem_segments",
                       os.path.join(output_path, "test_data_segments"))


def export_small_area_data(spark):
    logger.info("Processing small area data")
    # get vehicles IDs that crossed a bounding box
    sql = """SELECT DISTINCT vehicle_id
             FROM point
             WHERE latitude > {} AND latitude < {} AND longitude > {} AND longitude < {}""" \
        .format(bbox["min_y"], bbox["max_y"], bbox["min_x"], bbox["max_x"])
    vehicle_df = spark.sql(sql)
    vehicle_df.createOrReplaceTempView("vehicle")
    # filter by vehicle_id
    sql = """SELECT point.vehicle_id,
                    row_number() OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) AS point_id,
                    longitude,
                    latitude,
                    speed,
                    bearing,
                    time_utc,
                    unix_time,
                    unix_time - lag(unix_time) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) as prev_time_gap_s,
                    lead(unix_time) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) - unix_time as next_time_gap_s,
                    case when lag(geom) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) is not null
                         then st_distance(ST_Transform(geom, 'epsg:4326','epsg:3577'), lag(ST_Transform(geom, 'epsg:4326','epsg:3577')) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc))
                         else null end as prev_distance_m,
                    case when lead(geom) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) is not null
                         then st_distance(ST_Transform(geom, 'epsg:4326','epsg:3577'), lead(ST_Transform(geom, 'epsg:4326','epsg:3577')) OVER (PARTITION BY point.vehicle_id ORDER BY time_utc))
                         else null end as next_distance_m,
                    concat('SRID=4326;POINT (', longitude, ' ', latitude, ')') as geom
             FROM point
             INNER JOIN vehicle ON point.vehicle_id = vehicle.vehicle_id"""
    area_df = spark.sql(sql)
    # export df to postgres
    export_to_postgres(area_df, "testing.test_oem_small_area_points",
                       os.path.join(output_path, "test_data_small_area"))


def export_single_id_data(spark):
    logger.info("Processing single ID data")
    # filter by single ID
    sql = """SELECT vehicle_id,
                    row_number() OVER (PARTITION BY vehicle_id ORDER BY time_utc) AS point_id,
                    longitude,
                    latitude,
                    speed,
                    bearing,
                    time_utc,
                    unix_time,
                    unix_time - lag(unix_time) OVER (PARTITION BY vehicle_id ORDER BY time_utc) as prev_time_gap_s,
                    lead(unix_time) OVER (PARTITION BY vehicle_id ORDER BY time_utc) - unix_time as next_time_gap_s,
                    case when lag(geom) OVER (PARTITION BY vehicle_id ORDER BY time_utc) is not null 
                         then st_distance(ST_Transform(geom, 'epsg:4326','epsg:3577'), lag(ST_Transform(geom, 'epsg:4326','epsg:3577')) OVER (PARTITION BY vehicle_id ORDER BY time_utc))
                         else null end as prev_distance_m,
                    case when lead(geom) OVER (PARTITION BY vehicle_id ORDER BY time_utc) is not null 
                         then st_distance(ST_Transform(geom, 'epsg:4326','epsg:3577'), lead(ST_Transform(geom, 'epsg:4326','epsg:3577')) OVER (PARTITION BY vehicle_id ORDER BY time_utc))
                         else null end as next_distance_m,
                    concat('SRID=4326;POINT (', longitude, ' ', latitude, ')') as geom
             FROM point
             WHERE vehicle_id = '{}'""" \
        .format(vehicle_id)
    id_df = spark.sql(sql)
    # id_df.printSchema()
    # id_df.show()
    # export df to postgres
    export_to_postgres(id_df, "testing.test_oem_single_id_points",
                       os.path.join(output_path, "test_data_single_id"))


#TODO: account for the gaps before and after stops

def export_trip_and_stop_data(spark, schema_df=None):
    start_time = datetime.now()

    # schema_df = spark.read.parquet(os.path.join(output_path, "step_1_schema_applied"))
    # schema_df.createOrReplaceTempView("point")

    # # get vehicles IDs that crossed a bounding box
    # sql = """SELECT DISTINCT vehicle_id
    #          FROM point
    #          WHERE latitude > {} AND latitude < {} AND longitude > {} AND longitude < {}""" \
    #     .format(bbox["min_y"], bbox["max_y"], bbox["min_x"], bbox["max_x"])
    # vehicle_df = spark.sql(sql)
    # vehicle_df.createOrReplaceTempView("vehicle")

    # # get points
    # sql = """WITH veh AS (
    #              SELECT point.vehicle_id,
    #                     row_number() OVER (PARTITION BY point.vehicle_id ORDER BY time_utc) AS point_id,
    #                     -- longitude,
    #                     -- latitude,
    #                     concat(longitude, ' ', latitude) as coords,
    #                     speed,
    #                     bearing,
    #                     time_utc,
    #                     unix_time,
    #                     CASE WHEN speed > 10 THEN false ELSE true END AS is_stopped  -- flag when vehicle is stationary
    #              FROM point
    #          ), flag as (
    #              SELECT *,
    #                     CASE WHEN lag(is_stopped) OVER (PARTITION BY vehicle_id ORDER BY time_utc) = is_stopped
    #                          THEN 0 ELSE 1
    #                     END AS stop_move_flag  -- flag when vehicle starts moving or stops
    #              FROM veh
    #          )
    #          -- aggregate stop-move flag into running values to enable the segmentation below
    #          SELECT *,
    #                 sum(stop_move_flag) OVER (PARTITION BY vehicle_id ORDER BY time_utc) AS segment_id
    #          FROM flag"""
    # temp_stop_move_df = spark.sql(sql).repartition(f.to_date(f.col("time_utc")))
    # # stop_move_df.printSchema()
    # # stop_move_df.show(10, False)
    #
    # temp_stop_move_df.write.option("compression", "gzip") \
    #     .mode("overwrite") \
    #     .parquet(os.path.join(output_path, "step_2_stop_move"))
    #
    # schema_df.unpersist()
    # temp_stop_move_df.unpersist()
    #
    # stop_move_df = spark.read.parquet(os.path.join(output_path, "step_2_stop_move"))
    #
    # logger.info("Step 2 : {} stops & trips loaded : {}".format(stop_move_df.count(), datetime.now() - start_time))
    # start_time = datetime.now()
    #
    # w = Window.partitionBy("vehicle_id", "segment_id").orderBy("time_utc")
    #
    # # get durations of stop times to change short stop points to trip points
    # temp_short_stop_df = (stop_move_df.filter((f.col("is_stopped") == True) & (f.col("segment_id") > 1))
    #                  .groupBy("vehicle_id", "segment_id")
    #                  .agg(f.count(f.lit(1)).alias("point_count"),  # lit(1) is a hack to count rows
    #                       f.min("point_id").alias("min_point_id"),
    #                       f.max("point_id").alias("max_point_id"),
    #                       f.min("unix_time").alias("min_unix_time"),
    #                       f.max("unix_time").alias("max_unix_time")
    #                       )
    #                  .withColumn("duration_s", f.col("max_unix_time") - f.col("min_unix_time"))
    #                  .filter(f.col("duration_s") < 600)
    #                  .repartition(f.to_date(f.from_unixtime(f.col("min_unix_time"))))
    #                  )
    # temp_short_stop_df.createOrReplaceTempView("short_stop")
    # # temp_short_stop_df.printSchema()
    # # temp_short_stop_df.show(10)
    #
    # temp_short_stop_df.write.option("compression", "gzip") \
    #     .mode("overwrite") \
    #     .parquet(os.path.join(output_path, "step_3_short_stop"))
    #
    # stop_move_df.unpersist()
    # temp_short_stop_df.unpersist()
    #
    # short_stop_df = spark.read.parquet(os.path.join(output_path, "step_3_short_stop"))
    # # short_stop_df.createOrReplaceTempView("short_stop")
    #
    # logger.info("Step 3 : {} short stops created : {}".format(short_stop_df.count(), datetime.now() - start_time))
    # start_time = datetime.now()
    #
    # # stop_move_df.filter(f.col("segment_id") == 1).show()
    # # short_stop_df.show(10)
    #
    # # # flag short stops as false -- use inner join for performance - need to delete records in original before appending
    # # flag short stops as false
    # temp_short_stop_fixes_df = stop_move_df.alias("left") \
    #     .join(short_stop_df.alias("right"), on=(stop_move_df.vehicle_id == short_stop_df.vehicle_id) & (stop_move_df.segment_id == short_stop_df.segment_id), how="left") \
    #     .select("left.*", f.when(~f.isnull(f.col("right.point_count")), False).otherwise(f.col("left.is_stopped")).alias("is_stopped_fixed")) \
    #     .drop("stop_move_flag") \
    #     .drop("segment_id") \
    #     .repartition(f.to_date(f.col("time_utc")))
    # # temp_short_stop_fixes_df.printSchema()
    # # temp_short_stop_fixes_df.show(10, False)
    # # temp_short_stop_fixes_df.filter((f.col("vehicle_id") == "<something>") & (f.col("segment_id") == 2)).show()
    #
    # temp_short_stop_fixes_df.write.option("compression", "gzip") \
    #     .mode("overwrite") \
    #     .parquet(os.path.join(output_path, "step_4_short_stop_fixes"))
    #
    # short_stop_df.unpersist()
    # temp_short_stop_fixes_df.unpersist()
    #
    # short_stop_fixes_df = spark.read.parquet(os.path.join(output_path, "step_4_short_stop_fixes"))
    # short_stop_fixes_df.createOrReplaceTempView("short_stop_fixes")
    #
    # logger.info("Step 4 : {} short stops fixes created : {}".format(short_stop_fixes_df.count(), datetime.now() - start_time))
    # start_time = datetime.now()
    #
    # # redo the stop/move flags
    # sql = """WITH flag as (
    #              SELECT *,
    #                     CASE WHEN lag(is_stopped_fixed) OVER (PARTITION BY vehicle_id ORDER BY time_utc) = is_stopped_fixed
    #                          THEN 0 ELSE 1
    #                     END AS stop_move_flag  -- flag when vehicle starts moving or stops
    #              FROM short_stop_fixes
    #          )
    #          -- aggregate stop-move flag into running values to enable the segmentation below
    #          SELECT *,
    #                 sum(stop_move_flag) OVER (PARTITION BY vehicle_id ORDER BY time_utc) AS segment_id
    #          FROM flag"""
    # temp_stop_move_df2 = spark.sql(sql).repartition(f.to_date(f.col("time_utc")))
    # # temp_stop_move_df2.printSchema()
    # # temp_stop_move_df2.show(10, False)
    #
    # temp_stop_move_df2.write.option("compression", "gzip") \
    #     .mode("overwrite") \
    #     .parquet(os.path.join(output_path, "step_5_stop_move_2"))
    #
    # short_stop_fixes_df.unpersist()
    # temp_stop_move_df2.unpersist()
    #
    # stop_move_df2 = spark.read.parquet(os.path.join(output_path, "step_5_stop_move_2"))
    # stop_move_df2.createOrReplaceTempView("stop_move_2")
    #
    # logger.info("Step 5 : {} short stop moves fixed created : {}".format(stop_move_df2.count(), datetime.now() - start_time))
    # start_time = datetime.now()
    #
    # # w = Window.partitionBy("vehicle_id", "segment_id").orderBy("time_utc")
    # w = Window.orderBy("time_utc")
    #
    # # get min & max point_ids and times; and aggregate coords (in order) as a precursor to creating a trajectory geom
    # #   - note: you can't simplify the ordering of the collect_list to a single line: .agg(F.collect_list('coords').over(w))
    # temp_aggs_df = (stop_move_df2
    #                 .groupBy("vehicle_id", "is_stopped_fixed", "segment_id")
    #                 .agg(f.count(f.lit(1)).alias("point_count"),  # lit(1) is a hack to count rows
    #                      f.min("point_id").alias("min_point_id"),
    #                      f.max("point_id").alias("max_point_id"),
    #                      f.min("time_utc").alias("min_time_utc"),
    #                      f.max("time_utc").alias("max_time_utc"),
    #                      f.min("unix_time").alias("min_unix_time"),
    #                      f.max("unix_time").alias("max_unix_time"),
    #                      f.sort_array(f.collect_list(f.struct("point_id", "coords"))).alias("coord_array")
    #                      # f.max("coord_array").alias("coord_array") # max is used to get the last coord array -- needed!
    #                      )
    #                 .repartition(f.to_date(f.col("min_time_utc")))
    #            )
    #
    # # .withColumn("coord_array", f.collect_list("coords").over(w))  # array of coords in order (note: collect_set doesn't honour the order)
    #
    # # temp_aggs_df.printSchema()
    # # temp_aggs_df.select("coord_array").show(1, False)
    #
    # temp_aggs_df.write.option("compression", "gzip") \
    #     .mode("overwrite") \
    #     .parquet(os.path.join(output_path, "step_6_aggs"))
    #
    # stop_move_df2.unpersist()
    # temp_aggs_df.unpersist()

    aggs_df = spark.read.parquet(os.path.join(output_path, "step_6_aggs"))

    logger.info("Step 6 : {} stop trip aggregates created : {}".format(aggs_df.count(), datetime.now() - start_time))
    start_time = datetime.now()

    # create trip or stop duration and linestrings and points
    temp_trips_stops_df = (aggs_df
                           .withColumn("duration_s", aggs_df["max_unix_time"] - aggs_df["min_unix_time"])
                           .withColumn("wkt_geom", f.concat(f.when(aggs_df.point_count > 1, f.lit("LINESTRING("))
                                                        .otherwise(f.lit("POINT(")),
                                                        f.concat_ws(",", aggs_df["coord_array.coords"]),
                                                        f.lit(")")
                                                        )
                                       )
                           .withColumn("geom", f.expr("ST_GeomFromWKT(wkt_geom)"))
                           .drop("wkt_geom")
                           .drop("min_unix_time")
                           .drop("max_unix_time")
                           .drop("coord_array")
                           .repartition(f.to_date(f.col("min_time_utc")))
                           )
    # temp_trips_stops_df.printSchema()
    # temp_trips_stops_df.show(1, False)

    temp_trips_stops_df.write.option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(os.path.join(output_path, "step_7_trips_stops"))

    aggs_df.unpersist()
    temp_trips_stops_df.unpersist()

    trips_stops_df = spark.read.parquet(os.path.join(output_path, "step_7_trips_stops"))

    logger.info("Step 7 : {} trips and stops created: {}".format(trips_stops_df.count(), datetime.now() - start_time))
    start_time = datetime.now()

    # create trip or stop duration and linestrings and points
    temp_trips_stops_final_df = (trips_stops_df
                                 .withColumn("distance_km", f.expr("ST_Length(ST_Transform(geom, 'epsg:4326','epsg:3577')) / 1000.0"))
                                 .withColumn("avg_seconds_per_point", f.when(f.col("duration_s") > 0, f.col("duration_s").cast(t.DoubleType()) / trips_stops_df.point_count.cast(t.DoubleType())))
                                 .withColumn("avg_speed_kmh", f.when(f.col("duration_s") > 0, f.col("distance_km") / f.col("duration_s").cast(t.DoubleType()) / 0.0036))
                                 .withColumn("ewkt_geom", f.expr("concat('SRID=4326;', ST_AsText(geom))"))
                                 .drop("geom")
                                 .repartition(f.to_date(f.col("min_time_utc")))
                                 .cache()
                                 )
    # temp_trips_stops_final_df.printSchema()
    # temp_trips_stops_final_df.show(1, False)

    temp_trips_stops_final_df.write.option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(os.path.join(output_path, "step_8_trips_stops_final"))

    trips_stops_df.unpersist()
    temp_trips_stops_final_df.unpersist()

    trips_stops_final_df = spark.read.parquet(os.path.join(output_path, "step_8_trips_stops_final"))

    logger.info("Step 8 : {} trip and stop supporting fields created: {}".format(trips_stops_df.count(), datetime.now() - start_time))

    # export trip df to postgres
    logger.info("Step 9 : Exporting trips to Postgres")
    export_to_postgres(trips_stops_final_df.filter(trips_stops_final_df.is_stopped_fixed == False).drop("is_stopped_fixed"),
                       "testing.test_oem_trip", os.path.join(output_path, "step_8_test_data_trip"))

    # export stop df to postgres
    logger.info("Step 10 : Exporting stops to Postgres")
    export_to_postgres(trips_stops_final_df.filter(trips_stops_final_df.is_stopped_fixed == True).drop("is_stopped_fixed"),
                       "testing.test_oem_stop", os.path.join(output_path, "step_9_test_data_stop"))


# exports a DataFrame to Postgres (via CSV files saved to disk)
def export_to_postgres(df, table_name, csv_folder, partition_column=None):
    start_time = datetime.now()

    # get Postgres connection & cursor
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # # potentially expensive way to get number of DataFrame partitions!
    # num_partitions = df.rdd.getNumPartitions()

    # write to csv files - one gets written per partition
    # quotes are automatically put around strings with commas - i.e. csv is a safe export format
    if partition_column is not None:
        df.write.partitionBy(partition_column).csv(csv_folder, mode="overwrite", header=False, emptyValue="")
    else:
        df.write.csv(csv_folder, mode="overwrite", header=False, emptyValue="")

    # logger.info("exported dataframe to {:,} CSV files : {}"
    #     .format(num_partitions, datetime.now() - start_time))
    logger.info("\t - exported DataFrame to CSV files : {}".format(datetime.now() - start_time))
    start_time = datetime.now()

    pg_cur.execute("TRUNCATE TABLE {}".format(table_name))

    # get all CSV file paths and copy CSV files to Postgres using multiprocessing
    file_list = list()

    if partition_column is not None:
        search_path = "{}/*/*/*.csv".format(csv_folder)
    else:
        search_path = "{}/*.csv".format(csv_folder)

    for file_name in glob.glob(search_path):
        file_list.append(file_name)

    with Pool(num_processors) as p:
        p.starmap(execute_copy, zip(file_list, repeat(table_name)))

    pg_cur.execute("ANALYSE {}".format(table_name))

    pg_cur.close()
    pg_pool.putconn(pg_conn)

    # logger.info("copied {:,} CSV files to {} : {}"
    #             .format(num_partitions, table_name, datetime.now() - start_time))
    logger.info("\t - imported CSV files to {} : {}"
                .format(table_name, datetime.now() - start_time))


def execute_copy(file_name, table_name):
    # get postgres connection from pool
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # Use a SQL statement. The Psycopg2 copy_from function has issues with quotes in CSV files
    sql = """COPY {}
             FROM '{}'
             WITH (DELIMITER ',', FORMAT CSV, NULL '')""". format(table_name, file_name)
    pg_cur.execute(sql)

    pg_cur.close()
    pg_pool.putconn(pg_conn)


if __name__ == "__main__":
    full_start_time = datetime.now()

    # setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # set Spark logging levels
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # set logger
    log_file = os.path.abspath(__file__).replace(".py", ".log")
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                        datefmt="%m/%d/%Y %I:%M:%S %p")

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

    task_name = "Sedona testing"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
