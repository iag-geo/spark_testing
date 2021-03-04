
# script to load gnaf points & psma boundaries from Postgres to test Apache Sedona spatial join performance

import logging
import os
import pkg_resources
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark import StorageLevel
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession

# setup logging - code is here to prevent conflict with logging.basicConfig() from one of the imports below
log_file = os.path.abspath(__file__).replace(".py", ".log")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p")

from geospark.core.enums import GridType, IndexType, FileDataSplitter  # need to install geospark package
from geospark.core.spatialOperator import JoinQuery
from geospark.core.SpatialRDD import PointRDD
from geospark.register import upload_jars, GeoSparkRegistrator
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator
from geospark.utils.adapter import Adapter

# set number of parallel processes (sets number of Spark executors and Postgres concurrent connections)
num_processors = cpu_count()

# set Postgres parameters
pg_settings = {'HOST': 'localhost', 'DB': 'geo', 'PORT': '5432', 'USER': 'postgres', 'PASS': 'password'}

# create Postgres JDBC url
jdbc_url = "jdbc:postgresql://{HOST}:{PORT}/{DB}".format(**pg_settings)

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data")

# GNAF csv file
gnaf_csv_file_path = os.path.join(output_path, "gnaf_light.csv")

# list of input boundary Postgres tables
# bdy_list = [{"name": "local_government_areas", "id_field": "lga_pid", "name_field": "lga_name"}]
bdy_list = [{"name": "commonwealth_electorates", "id_field": "ce_pid", "name_field": "ce_name"},
            {"name": "local_government_areas", "id_field": "lga_pid", "name_field": "lga_name"},
            {"name": "local_government_wards", "id_field": "ward_pid", "name_field": "ward_name"},
            {"name": "state_lower_house_electorates", "id_field": "se_lower_pid", "name_field": "se_lower_name"},
            {"name": "state_upper_house_electorates", "id_field": "se_upper_pid", "name_field": "se_upper_name"}]


def main():

    rdd_flatmap_join()
    # rdd_filesave_join()


def rdd_filesave_join():
    logger.info("\t - RDD file save join start")

    full_start_time = datetime.now()

    # ----------------------------------------------------------
    # get spark session and context
    # ----------------------------------------------------------

    start_time = datetime.now()

    spark = create_spark_session()
    sc = spark.sparkContext
    sedona_version = pkg_resources.get_distribution("geospark").version

    logger.info("\t - PySpark {} session initiated with Apache Sedona {}: {}"
                .format(sc.version, sedona_version, datetime.now() - start_time))

    # ----------------------------------------------------------
    # create GNAF PointRDD from CSV file
    # ----------------------------------------------------------

    start_time = datetime.now()

    offset = 0  # The point long/lat fields start at column 0
    carry_other_attributes = True  # include non-geo columns

    point_rdd = PointRDD(sc, os.path.join(output_path, gnaf_csv_file_path),
                         offset, FileDataSplitter.CSV, carry_other_attributes)
    point_rdd.analyze()

    # add partitioning and indexing
    point_rdd.spatialPartitioning(GridType.KDBTREE)
    point_rdd.buildIndex(IndexType.RTREE, True)

    # set Spark storage type - set to MEMORY_AND_DISK if low on memory
    point_rdd.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

    logger.info("\t\t - GNAF RDD created: {}".format(datetime.now() - start_time))

    # ----------------------------------------------------------
    # get boundary tags using a spatial join
    # ----------------------------------------------------------

    for bdy in bdy_list:
        start_time = datetime.now()

        # load boundaries
        # create geometries from WKT strings into new DataFrame
        bdy_df = spark.read.parquet(os.path.join(output_path, bdy["name"])) \
            .withColumn("geom", f.expr("st_geomFromWKT(wkt_geom)")) \
            .drop("wkt_geom")

        # create bdy rdd
        bdy_rdd = Adapter.toSpatialRdd(bdy_df, "geom")
        bdy_rdd.analyze()

        bdy_df.unpersist()

        bdy_rdd.spatialPartitioning(point_rdd.getPartitioner())
        bdy_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)  # no need to persist(?) - used once

        # run the join - returns a PairRDD with 1 boundary to 1-N points
        # e.g. [Geometry: Polygon userData: WA32       TANGNEY WA, [Geometry: Point userData: GAWA_146792426	WA, ...]]
        result_pair_rdd = JoinQuery.SpatialJoinQueryFlat(point_rdd, bdy_rdd, True, True)
        # jim = result_pair_rdd.take(10)
        # for row in jim:
        #     print(row)

        result_pair_rdd.saveAsTextFile(os.path.join(output_path, "rdd_file_save_gnaf_with_{}".format(bdy["name"])))

        # # flat map values to have one point to bdy matched pair
        # flat_mapped_rdd = result_pair_rdd.flatMapValues(lambda x: x)
        #
        # # map values to create RDD row of gnaf & bdy IDs, plus state data
        # mapped_rdd = flat_mapped_rdd.map(
        #     lambda x: [x[1].getUserData().split("\t")[0],
        #                x[0].getUserData().split("\t")[0],
        #                x[0].getUserData().split("\t")[1]]
        # )
        #
        # # convert result to a dataframe of the following shema
        # schema = t.StructType([t.StructField("gnaf_pid", t.StringType(), False),
        #                        t.StructField(bdy["id_field"], t.StringType(), False),
        #                        t.StructField(bdy["name_field"], t.StringType(), False)])
        #
        # join_df = spark.createDataFrame(mapped_rdd, schema)
        #
        # # save result to disk
        # join_df.write \
        #     .option("compression", "gzip") \
        #     .mode("overwrite") \
        #     .parquet(os.path.join(output_path, "rdd_file_save_gnaf_with_{}".format(bdy["name"])))

        logger.info("\t\t - GNAF points bdy tagged with {}: {}"
                    .format(bdy["name"], datetime.now() - start_time))

    # cleanup
    spark.stop()

    logger.info("\t - RDD file save join done: {}".format(datetime.now() - full_start_time))


def rdd_flatmap_join():
    logger.info("\t - RDD flat map join start")

    full_start_time = datetime.now()

    # ----------------------------------------------------------
    # get spark session and context
    # ----------------------------------------------------------

    start_time = datetime.now()

    spark = create_spark_session()
    sc = spark.sparkContext
    sedona_version = pkg_resources.get_distribution("geospark").version

    logger.info("\t - PySpark {} session initiated with Apache Sedona {}: {}"
                .format(sc.version, sedona_version, datetime.now() - start_time))

    # ----------------------------------------------------------
    # create GNAF PointRDD from CSV file
    # ----------------------------------------------------------

    start_time = datetime.now()

    offset = 0  # The point long/lat fields start at column 0
    carry_other_attributes = True  # include non-geo columns

    point_rdd = PointRDD(sc, os.path.join(output_path, gnaf_csv_file_path),
                         offset, FileDataSplitter.CSV, carry_other_attributes)
    point_rdd.analyze()

    # add partitioning and indexing
    point_rdd.spatialPartitioning(GridType.KDBTREE)
    point_rdd.buildIndex(IndexType.RTREE, True)

    # set Spark storage type - set to MEMORY_AND_DISK if low on memory
    point_rdd.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

    logger.info("\t\t - GNAF RDD created: {}".format(datetime.now() - start_time))

    # ----------------------------------------------------------
    # get boundary tags using a spatial join
    # ----------------------------------------------------------

    for bdy in bdy_list:
        start_time = datetime.now()

        # load boundaries
        # create geometries from WKT strings into new DataFrame
        bdy_df = spark.read.parquet(os.path.join(output_path, bdy["name"])) \
            .withColumn("geom", f.expr("st_geomFromWKT(wkt_geom)")) \
            .drop("wkt_geom")

        # create bdy rdd
        bdy_rdd = Adapter.toSpatialRdd(bdy_df, "geom")
        bdy_rdd.analyze()

        bdy_df.unpersist()

        # add partitioning and indexing
        bdy_rdd.spatialPartitioning(point_rdd.getPartitioner())
        bdy_rdd.buildIndex(IndexType.RTREE, True)

        # set Spark storage type - set to MEMORY_AND_DISK if low on memory
        bdy_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)  # no need to persist(?) - used once

        # run the join - returns a PairRDD with 1 boundary to 1-N points
        # e.g. [Geometry: Polygon userData: WA32      TANGNEY WA, [Geometry: Point userData: GAWA_146792426	WA, ...]]
        result_pair_rdd = JoinQuery.SpatialJoinQuery(point_rdd, bdy_rdd, True, True)

        # result_pair_rdd.saveAsTextFile(os.path.join(output_path, "rdd_gnaf_with_{}".format(bdy["name"])))

        # # flat map values to have one point to bdy matched pair
        # flat_mapped_rdd = result_pair_rdd.flatMapValues(lambda x: x)
        #
        # # map values to create RDD row of gnaf & bdy IDs, plus state data
        # mapped_rdd = flat_mapped_rdd.map(
        #     lambda x: [x[1].getUserData().split("\t")[0],
        #                x[0].getUserData().split("\t")[0],
        #                x[0].getUserData().split("\t")[1]]
        # )
        #
        # # convert result to a dataframe of the following shema
        # schema = t.StructType([t.StructField("gnaf_pid", t.StringType(), False),
        #                        t.StructField(bdy["id_field"], t.StringType(), False),
        #                        t.StructField(bdy["name_field"], t.StringType(), False)])
        #
        # join_df = spark.createDataFrame(mapped_rdd, schema)
        #
        # # save result to disk
        # join_df.write \
        #     .option("compression", "gzip") \
        #     .mode("overwrite") \
        #     .parquet(os.path.join(output_path, "rdd_gnaf_with_{}".format(bdy["name"])))

        logger.info("\t\t - GNAF points bdy tagged with {}: {}"
                    .format(bdy["name"], datetime.now() - start_time))

    # cleanup
    spark.stop()

    logger.info("\t - RDD flat map join done: {}".format(datetime.now() - full_start_time))


def create_spark_session():
    # upload Apache Sedona JARs
    upload_jars()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("query")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)
             .config("spark.cores.max", num_processors)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.driver.memory", "8g")
             .getOrCreate()
             )

    # Register Apache Sedona UDTs and UDFs
    GeoSparkRegistrator.registerAll(spark)

    # # set Sedona spatial indexing and partitioning config in Spark session
    # # (no effect on the "small" spatial join query in this script. May improve bigger queries)
    # spark.conf.set("geospark.global.index", "true")
    # spark.conf.set("geospark.global.indextype", "rtree")
    # spark.conf.set("geospark.join.gridtype", "kdbtree")

    return spark


if __name__ == "__main__":

    # setup logging to file and the console (screen)
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

    task_name = "Apache Sedona benchmark"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    logger.info("{} finished".format(task_name))
    print()
