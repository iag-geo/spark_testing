
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

computer = "macbook2-comparison"

num_processors = cpu_count()

# input path for gzipped parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"

# bdy table subdivision vertex limit
max_vertices_list = [200]

# number of partitions on both dataframes
num_partitions_list = [1000]

# number of times to repeat test
test_repeats = 10

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# setup test log file using a normal file to avoid Spark errors being written.
log_file_path = os.path.abspath(__file__).replace(".py", ".csv")

if os.path.isfile(log_file_path):
    log_file = open(log_file_path, "a")
else:
    log_file = open(log_file_path, "w")
    # log header for log file (so results cn be used in Excel/Tableau)
    log_file.write("computer,points,boundaries,max_vertices,partitions,processing_time\n")


def main():
    # warmup runs
    run_test("warmup1", min(num_partitions_list), 200)
    run_test("warmup2", max(num_partitions_list), 200)

    # main test runs
    for test_run in range(test_repeats):
        for num_partitions in num_partitions_list:
            for max_vertices in max_vertices_list:
                run_test(computer, num_partitions, max_vertices)


def run_test(test_name, num_partitions, max_vertices):

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
             .config("spark.executor.cores", 2)
             .config("spark.driver.memory", "8g")
             .getOrCreate()
             )

    # Add Sedona functions and types to Spark
    SedonaRegistrator.registerAll(spark)

    # set Sedona spatial indexing and partitioning config in Spark session
    # (slowed down the "small" spatial join query in this script. Might improve bigger queries)
    spark.conf.set("sedona.global.index", "true")
    spark.conf.set("sedona.global.indextype", "rtree")
    spark.conf.set("sedona.join.gridtype", "kdbtree")
    spark.conf.set("sedona.join.numpartition", num_partitions)
    spark.conf.set("sedona.join.indexbuildside", "right")
    spark.conf.set("sedona.join.spatitionside", "right")

    start_time = datetime.now()

    # load gnaf points and create geoms
    point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
                .select("gnaf_pid", "state", "geom")
                .withColumnRenamed("state", "gnaf_state")
                .repartition(num_partitions, "gnaf_state")
                )
    point_df.createOrReplaceTempView("pnt")

    # load boundaries and create geoms
    if max_vertices is not None:
        bdy_vertex_name = "{}_{}".format(bdy_name, max_vertices)
    else:
        bdy_vertex_name = bdy_name

    bdy_df = (spark.read.parquet(os.path.join(input_path, bdy_vertex_name))
              .select(bdy_id, "state", "geom")
              .repartition(num_partitions, "state")
              .cache()
              )
    bdy_count = bdy_df.count()
    bdy_df.createOrReplaceTempView("bdy")

    # run spatial join to boundary tag the points
    sql = """SELECT pnt.gnaf_pid, bdy.{}, bdy.state FROM bdy INNER JOIN pnt ON ST_Intersects(bdy.geom, pnt.geom)""" \
        .format(bdy_id)
    join_df = spark.sql(sql)

    join_df2 = (join_df
                # .filter((join_df["state"] == join_df["gnaf_state"]))
                .dropDuplicates(["gnaf_pid", bdy_id])
                .cache()
                )

    # output to files
    if "warmup" in test_name:
        name = "gnaf_sql_{}_{}_{}".format(bdy_id, max_vertices, num_partitions)

        (join_df2.repartition(50)
         .write
         .partitionBy("state")
         .option("compression", "gzip")
         .mode("overwrite")
         .parquet(os.path.join(output_path, name))
         )

    # output vars
    join_count = join_df2.count()
    time_taken = datetime.now() - start_time

    if "warmup" in test_name:
        print("{},{},{},{},{},{}"
              .format(test_name, join_count, bdy_count, max_vertices, num_partitions, time_taken))
    else:
        log_file.write("{},{},{},{},{},{}\n"
                       .format(test_name, join_count, bdy_count, max_vertices, num_partitions, time_taken))

    # cleanup
    spark.stop()


if __name__ == "__main__":
    main()
