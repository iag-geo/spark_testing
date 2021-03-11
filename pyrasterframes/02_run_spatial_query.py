
# script to load boundary & point data into Spark and run a spatial (point in polygon) query with the data

import logging
import os
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark.sql import SparkSession
from pyrasterframes.utils import find_pyrasterframes_assembly
from pyrasterframes import rasterfunctions as rf_funcs
from pyrasterframes import rf_types

# # REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# # set Conda environment vars for PySpark
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"] = "/Users/hugh.saalmans/spark-2.4.6-bin-hadoop2.7"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# os.environ["PYSPARK_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"

# input path for parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# number of CPUs to use in processing (defaults to 2x local CPUs)
num_processors = cpu_count()


def main():
    start_time = datetime.now()

    # reference PyRasterFrames JARs
    rf_jar = find_pyrasterframes_assembly()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("query")
             .config("spark.jars", rf_jar)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.executor.cores", 1)
             .config("spark.cores.max", num_processors)
             .config("spark.driver.memory", "8g")
             .config("spark.driver.maxResultSize", "1g")
             .withKryoSerialization()
             .getOrCreate()
             .withRasterFrames()
             )

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))
    start_time = datetime.now()

    # create coordinate reference system for spatial operations
    wgs84_crs = rf_funcs.rf_mk_crs("EPSG:4326")

    # load boundaries (geometries are Well Known Text strings)
    bdy_wkt_df = spark.read.parquet(os.path.join(input_path, "boundaries"))
    # bdy_wkt_df.printSchema()
    # bdy_wkt_df.show(5)

    # create geometries from WKT strings into new DataFrame
    # new DF will be spatially indexed automatically
    bdy_df = bdy_wkt_df.withColumn("geom", rf_funcs.st_polygonFromText("wkt_geom")) \
        .drop("wkt_geom") \
        .withColumn("geom_index", rf_funcs.rf_xz2_index("geom", wgs84_crs, 18)) \
        .repartition("geom_index")

    # repartition and cache for performance (no effect on the "small" spatial join query here)
    # bdy_df.repartition(spark.sparkContext.defaultParallelism).cache().count()
    # bdy_df.printSchema()
    # bdy_df.show(5)

    logger.info("\t - Loaded and spatially enabled {:,} boundaries: {}"
                .format(bdy_df.count(), datetime.now() - start_time))
    start_time = datetime.now()

    # load points (spatial data is lat/long fields)
    point_wkt_df = spark.read.parquet(os.path.join(input_path, "points"))
    # point_wkt_df.printSchema()
    # point_wkt_df.show(5)

    # create point geometries from lat/long fields into new DataFrame
    point_df = point_wkt_df.withColumn("geom", rf_funcs.st_makePoint("longitude", "latitude")) \
        .drop("wkt_geom") \
        .withColumn("geom_index", rf_funcs.rf_z2_index("geom", wgs84_crs, 18)) \
        .repartition("geom_index")

    # repartition and cache for performance (no effect on the "small" spatial join query here)
    # point_df.repartition(spark.sparkContext.defaultParallelism).cache().count()
    # point_df.printSchema()
    # point_df.show(5)

    logger.info("\t - Loaded and spatially enabled {:,} points: {}"
                .format(point_df.count(), datetime.now() - start_time))
    start_time = datetime.now()

    # run spatial join to boundary tag the points
    # notes:
    #   - it's an inner join so point records could be lost
    join_df = point_df.join(bdy_df, rf_funcs.st_intersects(point_df.geom, bdy_df.geom))
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

    task_name = "Sedona testing"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
