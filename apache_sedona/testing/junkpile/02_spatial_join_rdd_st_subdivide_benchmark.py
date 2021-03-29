
# script to benchmark spatial join performance between gnaf (14M records) and a national boundary dataset
# using RDDs instead of dataframes with SQL

import logging
import os
# import platform

from datetime import datetime
from multiprocessing import cpu_count
# from pyspark.sql import functions as f
from pyspark.sql import SparkSession

# setup logging - code is here to prevent conflict with logging.basicConfig() from one of the imports below
log_file = os.path.abspath(__file__).replace(".py", ".csv")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(message)s")

from sedona.core.enums import GridType, IndexType
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.utils.adapter import Adapter

computer = "macbook2-no-partition"

num_processors = cpu_count()

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


def main():
    # log header for log file (so results cn be used in Excel/Tableau)
    logger.info("computer,points,boundaries,max_vertices,partitions,processing_time")

    # warmup runs
    join_count, bdy_count, time_taken = run_test(min(num_partitions_list), max(max_vertices_list))
    print("{},{},{},{},{},{}"
          .format("warmup1", join_count, bdy_count, max(max_vertices_list), min(num_partitions_list), time_taken))

    # join_count, bdy_count, time_taken = run_test(max(num_partitions_list), min(max_vertices_list))
    # print("{},{},{},{},{},{}"
    #       .format("warmup2", join_count, bdy_count, min(max_vertices_list), max(num_partitions_list), time_taken))

    # main test runs
    for num_partitions in num_partitions_list:
        for max_vertices in max_vertices_list:
            join_count, bdy_count, time_taken = run_test(num_partitions, max_vertices)
            logging.info("{},{},{},{},{},{}"
                         .format(computer, join_count, bdy_count, max_vertices, num_partitions, time_taken))


def run_test(num_partitions, max_vertices):

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
             # .config("spark.sql.adaptive.enabled", "true")
             .config("spark.executor.cores", 2)
             # .config("spark.cores.max", num_processors)
             .config("spark.driver.memory", "8g")
             # .config("spark.driver.maxResultSize", "2g")
             .getOrCreate()
             )

    # spark.sparkContext.setLogLevel("ERROR")

    # Add Sedona functions and types to Spark
    SedonaRegistrator.registerAll(spark)

    start_time = datetime.now()

    # load gnaf points and create geoms
    point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
                # .select("gnaf_pid", "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
                # .limit(5000)
                .repartition(num_partitions, "state")
                # .cache()
                )

    # print("point_df : {} partitions".format(point_df.rdd.getNumPartitions()))

    # # get column names
    # point_columns = point_df.schema.names
    # print(point_columns)

    # load boundaries and create geoms
    if max_vertices is not None:
        bdy_vertex_name = "{}_{}".format(bdy_name, max_vertices)
    else:
        bdy_vertex_name = bdy_name

    bdy_df = (spark.read.parquet(os.path.join(input_path, bdy_vertex_name))
              # .withColumnRenamed("gid", "id")
              # .select(bdy_id, "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
              .repartition(num_partitions, "state")
              # .cache()
              )

    # print("bdy_df : {} partitions".format(bdy_df.rdd.getNumPartitions()))

    # # get column names
    # bdy_columns = bdy_df.schema.names
    # print(bdy_columns)

    # create RDDs - analysed partitioned and indexed
    point_rdd = Adapter.toSpatialRdd(point_df, "geom")
    bdy_rdd = Adapter.toSpatialRdd(bdy_df, "geom")

    point_rdd.analyze()
    bdy_rdd.analyze()

    point_rdd.spatialPartitioning(GridType.KDBTREE)
    bdy_rdd.spatialPartitioning(point_rdd.getPartitioner())

    point_rdd.buildIndex(IndexType.RTREE, True)
    bdy_rdd.buildIndex(IndexType.RTREE, True)

    # print("point_rdd : {} partitions".format(point_rdd.getRawJvmSpatialRDD().getNumPartitions()))
    # print("bdy_rdd : {} partitions".format(bdy_rdd.getRawJvmSpatialRDD().getNumPartitions()))

    # doesn't currently work (Sedona v1.0.1 SNAPSHOT - 2021-03-23)
    # bdy_rdd.indexedRawRDD.saveAsObjectFile(os.path.join(output_path, "{}_rdd".format(bdy_vertex_name)))

    # print(point_rdd.fieldNames)
    # print(bdy_rdd.fieldNames)

    # run join query
    join_pair_rdd = JoinQueryRaw.SpatialJoinQueryFlat(point_rdd, bdy_rdd, True, True)

    # convert SedonaPairRDD to dataframe
    join_df = Adapter.toDf(join_pair_rdd, bdy_rdd.fieldNames, point_rdd.fieldNames, spark)
    # join_df.printSchema()
    # join_df.show(10)

    # | -- leftgeometry: geometry(nullable=true)
    # | -- gid: string(nullable=true)
    # | -- ce_pid: string(nullable=true)
    # | -- state: string(nullable=true)
    # | -- rightgeometry: geometry(nullable=true)
    # | -- gid: string(nullable=true)
    # | -- gnaf_pid: string(nullable=true)
    # | -- state: string(nullable=true)

    # output vars
    join_count = join_df.count()
    bdy_count = bdy_df.count()
    time_taken = datetime.now() - start_time

    # cleanup
    spark.stop()

    return join_count, bdy_count, time_taken


if __name__ == "__main__":
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
