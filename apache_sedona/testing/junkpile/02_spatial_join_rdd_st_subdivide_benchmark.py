
# script to benchmark spatial join performance between gnaf (14M records) and a national boundary dataset
# using RDDs instead of dataframes with SQL

import os

from datetime import datetime
from pyspark.sql import SparkSession

from sedona.core.enums import GridType, IndexType
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.utils.adapter import Adapter

computer = "macbook2-refactored"

# input path for gzipped parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"

# bdy table subdivision vertex limit
max_vertices_list = [25]

# number of partitions on both dataframes
num_partitions_list = [200]

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
    run_test("warmup1", min(num_partitions_list), max(max_vertices_list))
    run_test("warmup2", max(num_partitions_list), min(max_vertices_list))

    # main test runs
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

    start_time = datetime.now()

    # load gnaf points and create geoms
    point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
                .select("gnaf_pid", "state", "geom")
                .withColumnRenamed("state", "gnaf_state")
                .repartition(num_partitions, "gnaf_state")
                )

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

    # create RDDs - analysed partitioned and indexed
    point_rdd = Adapter.toSpatialRdd(point_df, "geom")
    bdy_rdd = Adapter.toSpatialRdd(bdy_df, "geom")

    point_rdd.analyze()
    bdy_rdd.analyze()

    point_rdd.spatialPartitioning(GridType.KDBTREE)
    bdy_rdd.spatialPartitioning(point_rdd.getPartitioner())

    point_rdd.buildIndex(IndexType.RTREE, True)
    bdy_rdd.buildIndex(IndexType.RTREE, True)

    # run join query
    join_pair_rdd = JoinQueryRaw.SpatialJoinQueryFlat(point_rdd, bdy_rdd, True, True)

    # convert SedonaPairRDD to dataframe
    join_df = Adapter.toDf(join_pair_rdd, bdy_rdd.fieldNames, point_rdd.fieldNames, spark)
    # join_df.printSchema()

    # | -- leftgeometry: geometry(nullable=true)
    # | -- <bdy_id>: string(nullable=true)
    # | -- state: string(nullable=true)
    # | -- rightgeometry: geometry(nullable=true)
    # | -- gnaf_pid: string(nullable=true)
    # | -- gnaf_state: string(nullable=true)

    join_df2 = (join_df
                # .filter((join_df["state"] == join_df["gnaf_state"]))
                .select("gnaf_pid", bdy_id, "state")
                .dropDuplicates(["gnaf_pid", bdy_id])
                .cache()
                )

    # output to files
    if "warmup" in test_name:
        name = "gnaf_rdd_{}_{}_{}".format(bdy_id, max_vertices, num_partitions)

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
