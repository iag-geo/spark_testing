
# script to benchmark spatial join performance between gnaf (14M records) and a national boundary dataset

import logging
import os
# import platform

from datetime import datetime
from multiprocessing import cpu_count
# from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

computer = "macbook2-different-partitions"

num_processors = cpu_count()

# input path for gzipped parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"

# bdy table subdivision vertex limit
max_vertices_list = [100, 200]

# number of partitions on both dataframes
num_gnaf_partitions_list = [250, 500, 750, 1000]
num_bdy_partitions_list = [250, 500, 750, 1000]

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")


def main():
    # log header for log file (so results cn be used in Excel/Tableau)
    logger.info("computer,points,boundaries,max_vertices,gnaf_partitions,bdy_partitions,processing_time")

    # warmup runs
    join_count, bdy_count, time_taken = \
        run_test(min(num_gnaf_partitions_list), min(num_bdy_partitions_list), max(max_vertices_list))
    print("{},{},{},{},{},{},{}"
          .format("warmup1", join_count, bdy_count, max(max_vertices_list),
                  min(num_gnaf_partitions_list), min(num_bdy_partitions_list), time_taken))

    join_count, bdy_count, time_taken = \
        run_test(max(num_gnaf_partitions_list), max(num_bdy_partitions_list), min(max_vertices_list))
    print("{},{},{},{},{},{},{}"
          .format("warmup2", join_count, bdy_count, min(max_vertices_list),
                  max(num_gnaf_partitions_list), max(num_bdy_partitions_list), time_taken))

    # main test runs
    for num_gnaf_partitions in num_gnaf_partitions_list:
        for num_bdy_partitions in num_bdy_partitions_list:
            for max_vertices in max_vertices_list:
                join_count, bdy_count, time_taken = run_test(num_gnaf_partitions, num_bdy_partitions, max_vertices)
                logging.info("{},{},{},{},{},{},{}"
                             .format(computer, join_count, bdy_count, max_vertices,
                                     num_gnaf_partitions, num_bdy_partitions, time_taken))


def run_test(num_gnaf_partitions, num_bdy_partitions, max_vertices):

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
             # .config("spark.executor.cores", 4)
             .config("spark.cores.max", num_processors)
             .config("spark.driver.memory", "8g")
             # .config("spark.driver.maxResultSize", "2g")
             .getOrCreate()
             )

    # Add Sedona functions and types to Spark
    SedonaRegistrator.registerAll(spark)

    start_time = datetime.now()

    # load gnaf points and create geoms
    point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
                # .select("gnaf_pid", "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
                # .limit(1000000)
                .repartition(num_gnaf_partitions, "state")
                # .cache()
                )
    point_df.createOrReplaceTempView("pnt")

    # load boundaries and create geoms
    bdy_vertex_name = "{}_{}".format(bdy_name, max_vertices)

    bdy_df = (spark.read.parquet(os.path.join(input_path, bdy_vertex_name))
              # .select(bdy_id, "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
              .repartition(num_bdy_partitions, "state")
              # .cache()
              )
    bdy_df.createOrReplaceTempView("bdy")

    # run spatial join to boundary tag the points
    sql = """SELECT pnt.gnaf_pid, bdy.{} FROM pnt INNER JOIN bdy ON ST_Intersects(pnt.geom, bdy.geom)""" \
        .format(bdy_id)
    join_df = spark.sql(sql)

    # output vars
    join_count = join_df.count()
    bdy_count = bdy_df.count()
    time_taken = datetime.now() - start_time

    # cleanup
    spark.stop()

    return join_count, bdy_count, time_taken


if __name__ == "__main__":
    # setup logging - code is here to prevent conflict with logging.basicConfig() from one of the imports below
    log_file = os.path.abspath(__file__).replace(".py", "_2.csv")
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(message)s")

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
    logging.getLogger().addHandler(console)

    main()
