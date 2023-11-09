
# script to load boundary & point data into Spark and run a spatial (point in polygon) query with the data

import boto3
import logging
import os
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark.sql import functions as f
from sedona.spark import *

#######################################################################################################################
# Set your parameters here
#######################################################################################################################

aws_profile = "minus34"

s3_path = "s3a://minus34.com/opendata/geoscape-202308/parquet/"

#######################################################################################################################

# number of CPUs to use in processing (defaults to number of local CPUs)
num_processors = cpu_count()


def main():
    start_time = datetime.now()

    # get AWS credentials
    session = boto3.Session(profile_name=aws_profile)
    credentials = session.get_credentials()

    # Credentials are refreshable, so accessing your access key / secret key
    # separately can lead to a race condition. Use this to get an actual matched
    # set.
    credentials = credentials.get_frozen_credentials()
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key

    # create spark session object
    config = (SedonaContext
              .builder()
              .master("local[*]")
              .appName("Sedona Test")
              .config("spark.sql.session.timeZone", "UTC")
              .config("spark.sql.debug.maxToStringFields", 100)
              .config("spark.sql.adaptive.enabled", "true")
              .config("spark.serializer", KryoSerializer.getName)
              .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
              .config("spark.executor.cores", 1)
              .config("spark.cores.max", num_processors)
              .config("spark.driver.memory", "4g")
              .config("spark.driver.maxResultSize", "2g")
              .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
              # .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
              .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
              .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
              .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
              .getOrCreate()
              )

    # Add Sedona functions and types to Spark
    spark = SedonaContext.create(config)

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))
    start_time = datetime.now()

    # load boundaries (geometries are Well Known Text strings)
    # bdy_wkt_df = spark.read.parquet(os.path.join(input_path, "boundaries"))
    bdy_df = spark.read.format("geoparquet").load(os.path.join(s3_path, "local_government_areas"))
    # bdy_df = bdy_df.repartition(96, "state")
    bdy_df.printSchema()
    bdy_df.show(5)
    # bdy_df.count()

    # address_aliases



    # # create view to enable SQL queries
    # bdy_wkt_df.createOrReplaceTempView("bdy_wkt")
    #
    # # create geometries from WKT strings into new DataFrame
    # # new DF will be spatially indexed automatically
    # sql = "select bdy_id, state, ST_GeomFromWKT(wkt_geom) as geometry from bdy_wkt"
    # bdy_df = spark.sql(sql).repartition(96, "state")
    #
    # # repartition and cache for performance (no effect on the "small" spatial join query here)
    # # bdy_df.repartition(spark.sparkContext.defaultParallelism).cache().count()
    # bdy_df.printSchema()
    # bdy_df.show(5)

    # create view to enable SQL queries
    bdy_df.createOrReplaceTempView("bdy")

    logger.info("\t - Loaded and spatially enabled {:,} boundaries: {}"
                .format(bdy_df.count(), datetime.now() - start_time))
    start_time = datetime.now()

    # # load points (spatial data is lat/long fields)
    # # point_wkt_df = spark.read.parquet(os.path.join(input_path, "points"))
    # point_df = spark.read.format("geoparquet").load(os.path.join(input_path, "points"))
    # point_df = point_df.repartition(96, "state")
    # # point_wkt_df.printSchema()
    # # point_wkt_df.show(5)
    #
    # # # create view to enable SQL queries
    # # point_wkt_df.createOrReplaceTempView("point_wkt")
    # #
    # # # create geometries from lat/long fields into new DataFrame
    # # # new DF will be spatially indexed automatically
    # # sql = "select point_id, state, ST_Point(longitude, latitude) as geometry from point_wkt"
    # # point_df = spark.sql(sql).repartition(96, "state")
    # #
    # # # repartition and cache for performance (no effect on the "small" spatial join query here)
    # # # point_df.repartition(spark.sparkContext.defaultParallelism).cache().count()
    # # point_df.printSchema()
    # # point_df.show(5)
    #
    # # create view to enable SQL queries
    # point_df.createOrReplaceTempView("pnt")
    #
    # logger.info("\t - Loaded and spatially enabled {:,} points: {}"
    #             .format(point_df.count(), datetime.now() - start_time))
    # start_time = datetime.now()
    #
    # # run spatial join to boundary tag the points
    # # notes:
    # #   - spatial partitions and indexes for join will be created automatically
    # #   - it's an inner join so point records could be lost
    # sql = """SELECT pnt.point_id,
    #                 bdy.bdy_id,
    #                 bdy.state,
    #                 pnt.geom
    #          FROM pnt
    #          INNER JOIN bdy ON ST_Intersects(pnt.geom, bdy.geom)"""
    # join_df = spark.sql(sql)
    # # join_df.explain()
    #
    # # # output join DataFrame
    # # join_df.write.option("compression", "gzip") \
    # #     .mode("overwrite") \
    # #     .parquet(os.path.join(input_path, "output"))
    #
    # num_joined_points = join_df.count()
    #
    # join_df.printSchema()
    # join_df.orderBy(f.rand()).show(5, False)
    #
    # logger.info("\t - {:,} points were boundary tagged: {}"
    #             .format(num_joined_points, datetime.now() - start_time))

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

    task_name = "Apache Sedona testing"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()