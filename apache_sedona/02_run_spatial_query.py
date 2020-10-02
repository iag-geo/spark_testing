
# script to load boundary & point data into Spark and run a spatial (point in polygon) query with the data

import logging
import os
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark.sql import SparkSession
from geospark.register import upload_jars, GeoSparkRegistrator
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator

# # REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# # set Conda environment vars for PySpark
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"] = "/Users/s57405/spark-2.4.6-bin-hadoop2.7"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# os.environ["PYSPARK_PYTHON"] = "/Users/s57405/opt/miniconda3/envs/geospark_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/s57405/opt/miniconda3/envs/geospark_env/bin/python"
# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"

# input path for parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# number of CPUs to use in processing (defaults to 2x local CPUs)
num_processors = cpu_count() * 2


def main():
    start_time = datetime.now()

    # upload Sedona (geospark) JARs
    # theoretically only need to do this once
    upload_jars()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("query")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.executor.cores", 1)
             .config("spark.cores.max", num_processors)
             .config("spark.driver.memory", "8g")
             .config("spark.driver.maxResultSize", "1g")
             .getOrCreate()
             )

    # Register Apache Sedona (geospark) UDTs and UDFs
    GeoSparkRegistrator.registerAll(spark)

    # set Sedona spatial indexing and partioning config in Spark session
    # (no effect on the "small" spatial join query in this script. Will improve bigger queries)
    spark.conf.set("geospark.global.index", "true")
    spark.conf.set("geospark.global.indextype", "kdbtree")
    spark.conf.set("geospark.join.gridtype", "kdbtree")

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))
    start_time = datetime.now()

    # load boundaries (geometries are Well Known Text strings)
    bdy_wkt_df = spark.read.parquet(os.path.join(input_path, "boundaries"))
    # bdy_wkt_df.printSchema()
    # bdy_wkt_df.show(5)

    # create view to enable SQL queries
    bdy_wkt_df.createOrReplaceTempView("bdy_wkt")

    # create geometries from WKT strings into new DataFrame
    # new DF will be spatially indexed automatically
    bdy_df = spark.sql("select bdy_id, st_geomFromWKT(wkt_geom) as geometry from bdy_wkt")

    # repartition and cache for performance (no effect on the "small" spatial join query here)
    # bdy_df.repartition(spark.sparkContext.defaultParallelism).cache().count()
    # bdy_df.printSchema()
    # bdy_df.show(5)

    # create view to enable SQL queries
    bdy_df.createOrReplaceTempView("bdy")

    logger.info("\t - Loaded and spatially enabled {:,} boundaries: {}"
                .format(bdy_df.count(), datetime.now() - start_time))
    start_time = datetime.now()

    # load points (spatial data is lat/long fields)
    point_wkt_df = spark.read.parquet(os.path.join(input_path, "points"))
    # point_wkt_df.printSchema()
    # point_wkt_df.show(5)

    # create view to enable SQL queries
    point_wkt_df.createOrReplaceTempView("point_wkt")

    # create geometries from lat/long fields into new DataFrame
    # new DF will be spatially indexed automatically
    sql = """select point_id, 
                    st_point(cast(longitude as decimal(9, 6)), cast(latitude as decimal(8, 6))) as geometry
             from point_wkt"""
    point_df = spark.sql(sql)

    # repartition and cache for performance (no effect on the "small" spatial join query here)
    # point_df.repartition(spark.sparkContext.defaultParallelism).cache().count()
    # point_df.printSchema()
    # point_df.show(5)

    # create view to enable SQL queries
    point_df.createOrReplaceTempView("pnt")

    logger.info("\t - Loaded and spatially enabled {:,} points: {}"
                .format(point_df.count(), datetime.now() - start_time))
    start_time = datetime.now()

    # run spatial join to boundary tag the points
    # notes:
    #   - spatial partitions and indexes for join will be created automatically
    #   - it's an inner join so point records could be lost
    sql = """SELECT pnt.point_id,
                    bdy.bdy_id, 
                    pnt.geometry
             FROM pnt
             INNER JOIN bdy ON ST_Intersects(pnt.geometry, bdy.geometry)"""
    join_df = spark.sql(sql)
    # join_df.explain()

    # # output join DataFrame
    # join_df.write.option("compression", "gzip") \
    #     .mode("overwrite") \
    #     .parquet(os.path.join(input_path, "output"))

    num_joined_points = join_df.count()

    join_df.printSchema()
    join_df.show(5)

    logger.info("\t - {:,} points were boundary tagged: {}"
                .format(num_joined_points, datetime.now() - start_time))

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

    task_name = "Geospark testing"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
