
# script to benchmark spatial join performance between gnaf and a national boundary dataset

import glob
import logging
import os
import sys

from datetime import datetime
from itertools import repeat
from multiprocessing import cpu_count, Pool

from pyspark.sql import functions as f
from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

num_processors = cpu_count() * 2

# input path for gzipped parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/hugh.saalmans/git/minus34/gnaf-loader/spark/data")

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"
num_partitions = 96

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data")


def main():
    # create spark session object
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("Spatial Join SQL Benchmark")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
             # .config("spark.jars.packages",
             #         'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,'
             #         'org.datasyslab:geotools-wrapper:geotools-24.0')
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.executor.cores", 1)
             .config("spark.cores.max", num_processors)
             .config("spark.driver.memory", "12g")
             # .config("spark.driver.maxResultSize", "2g")
             .getOrCreate()
             )

    # Add Sedona functions and types to Spark
    SedonaRegistrator.registerAll(spark)

    # # load gnaf points and create geoms
    point_df = spark.read.parquet(os.path.join(input_path, "address_principals")) \
        .select("gnaf_pid", "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom")) \
        .repartition(num_partitions, "state")

    point_df.createOrReplaceTempView("pnt")

    # load boundaries and create geoms
    bdy_df = spark.read.parquet(os.path.join(input_path, bdy_name)) \
        .withColumn("geom", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom")) \
        .repartition(num_partitions, "state")
    bdy_df.createOrReplaceTempView("bdy")
    # bdy_df.printSchema()

    # run spatial join to boundary tag the points
    # notes:
    #   - spatial partitions and indexes for join will be created automatically
    #   - it's an inner join so point records could be lost (left joins not yet supported by sedona)
    #   - force broadcast of unpartitioned boundaries (to speed up query) using /*+ BROADCAST(bdy) */

    sql = """SELECT pnt.gnaf_pid,
                    bdy.{}
                 FROM pnt
                 INNER JOIN bdy ON ST_Intersects(pnt.geom, bdy.geom)""".format(bdy_id)
    join_df = spark.sql(sql)

    # output join DataFrame
    export_to_parquet(join_df, "gnaf_with_{}".format(bdy_name))

    # cleanup
    spark.stop()


def export_to_parquet(df, name):
    df.write.option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(os.path.join(output_path, name))


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
    console.setLevel(logging.WARNING)
    # set a format which is simpler for console use
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)

    task_name = "Sedona SQL Benchmark"
    system_name = "mobility.ai"

    # logger.info("{} started".format(task_name))
    # logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("Finished {} : {}".format(task_name, time_taken))
    print()
