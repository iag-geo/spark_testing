
# script to load gnaf points from Postgres into CSV and Parquet

import logging
import os
import psycopg2
import shutil
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark import StorageLevel
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession

from geospark.core.enums import GridType, IndexType, FileDataSplitter  # need to install geospark package
from geospark.core.spatialOperator import JoinQuery
from geospark.core.SpatialRDD import PointRDD
from geospark.register import upload_jars, GeoSparkRegistrator
# from geospark.sql.types import GeometryType
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator
from geospark.utils.adapter import Adapter

# # REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# os.environ["JAVA_HOME"]="/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"]="/Users/hugh.saalmans/spark-3.0.1-bin-hadoop3.2"
# os.environ["SPARK_LOCAL_IP"]="127.0.0.1"
# os.environ["SPARK_LOCAL_DIRS"]="/Users/hugh.saalmans/tmp/spark"
# os.environ["PYSPARK_PYTHON"]="/Users/hugh.saalmans/opt/miniconda3/envs/geospark3_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/Users/hugh.saalmans/opt/miniconda3/envs/geospark3_env/bin/python"
# os.environ["PYLIB"]="${SPARK_HOME_DIR}/python/lib"

num_processors = cpu_count()


# get postgres parameters from local text file
# format per connection is:  server_name := HOST|hostname,DB|database,PORT|port_number,USER|username,PASS|password
def get_password(connection_name):
    passwords_file_path = os.path.join(os.environ["GIT_HOME"], "passwords.ini")

    if os.path.exists(passwords_file_path):
        passwords_file = open(passwords_file_path, 'r').read().splitlines()
        passwords_file = [i for i in passwords_file if len(i) != 0]  # remove empty lines
        passwords_file = [i for i in passwords_file if i[0] != "#"]  # remove comment lines

        params = dict()
        for ini in passwords_file:
            params[ini.split()[0].rstrip().lstrip()] = ini.split(':=')[1].rstrip().lstrip()

        return dict(item.split("|") for item in params[connection_name].split(","))


local_pg_settings = get_password("localhost_super")

# create postgres JDBC url
jdbc_url = "jdbc:postgresql://{HOST}:{PORT}/{DB}".format(**local_pg_settings)

# get connect string for psycopg2
local_pg_connect_string = "dbname={DB} host={HOST} port={PORT} user={USER} password={PASS}".format(**local_pg_settings)

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# gnaf csv file
input_file_name = os.path.join(output_path, "gnaf_light.csv")


def main():
    start_time = datetime.now()

    # # copy gnaf tables from Postgres to a CSV file - a one off
    # pg_conn = psycopg2.connect(local_pg_connect_string)
    # pg_cur = pg_conn.cursor()
    #
    # sql = """COPY (
    #              SELECT longitude, latitude, gnaf_pid, state
    #              FROM gnaf_202008.{}
    #          ) TO STDOUT WITH CSV"""
    # # sql = """COPY (
    # #              SELECT gnaf_pid, street_locality_pid, locality_pid, alias_principal, primary_secondary, building_name,
    # #                     lot_number, flat_number, level_number, number_first, number_last, street_name, street_type,
    # #                     street_suffix, address, locality_name, postcode, state, locality_postcode, confidence,
    # #                     legal_parcel_id, mb_2011_code, mb_2016_code, latitude, longitude, geocode_type, reliability
    # #              FROM gnaf_202008.{}
    # #          ) TO STDOUT WITH CSV"""
    #
    # # address principals
    # with open(input_file_name, 'w') as csv_file:
    #     pg_cur.copy_expert(sql.format("address_principals"), csv_file)
    #     # pg_cur.copy_expert(sql.format("address_principals") + " HEADER", csv_file)
    #
    # # address aliases
    # with open(input_file_name, 'a') as csv_file:
    #     pg_cur.copy_expert(sql.format("address_aliases"), csv_file)
    #
    # pg_cur.close()
    # pg_conn.close()
    #
    # logger.info("\t - GNAF points exported to CSV: {}".format( datetime.now() - start_time))
    # start_time = datetime.now()

    # upload Sedona (geospark) JARs
    upload_jars()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("compass_iot_query")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)
             .config("spark.cores.max", num_processors)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.driver.memory", "8g")
             .getOrCreate()
             )

    # .config("spark.kryoserializer.buffer.max", "2g")

    # Register Apache Sedona (geospark) UDTs and UDFs
    GeoSparkRegistrator.registerAll(spark)

    sc = spark.sparkContext

    logger.info("\t - PySpark {} session initiated: {}".format(sc.version, datetime.now() - start_time))
    start_time = datetime.now()

    offset = 0  # The point long/lat starts from Column 0
    carry_other_attributes = True  # include non-geo columns

    point_rdd = PointRDD(sc, os.path.join(output_path, input_file_name),
                         offset, FileDataSplitter.CSV, carry_other_attributes)

    point_rdd.analyze()

    # add partitioning and indexing to RDDs
    point_rdd.spatialPartitioning(GridType.KDBTREE)
    point_rdd.buildIndex(IndexType.RTREE, True)

    point_rdd.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

    logger.info("\t - GNAF points loaded: {}".format(datetime.now() - start_time))
    # logger.info("\t - {} GNAF points loaded: {}".format(gnaf_df.count(), datetime.now() - start_time))

    bdy_tag(spark, point_rdd, "commonwealth_electorates", "ce_pid")
    bdy_tag(spark, point_rdd, "local_government_areas", "lga_pid")
    bdy_tag(spark, point_rdd, "local_government_wards", "ward_pid")
    bdy_tag(spark, point_rdd, "state_lower_house_electorates", "se_lower_pid")
    bdy_tag(spark, point_rdd, "state_upper_house_electorates", "se_upper_pid")

    # cleanup
    spark.stop()


def bdy_tag(spark, point_rdd, bdy_name, bdy_id):
    start_time = datetime.now()

    # load boundaries
    bdy_rdd = export_bdys(spark, bdy_name, bdy_id)
    bdy_rdd.analyze()

    bdy_rdd.spatialPartitioning(point_rdd.getPartitioner())
    bdy_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    # run the join
    # returns [Geometry: Polygon userData: WA32       TANGNEY WA, [Geometry: Point userData: GAWA_146792426	WA, ...]]
    result_pair_rdd = JoinQuery.SpatialJoinQuery(point_rdd, bdy_rdd, True, True)
    # print(result_pair_rdd.take(1))

    # flat map values to have one point to bdy match
    flat_mapped_rdd = result_pair_rdd.flatMapValues(lambda x: x)

    # map values to create RDD row of point data and bdy ID
    mapped_rdd = flat_mapped_rdd.map(
        lambda x: [x[1].getUserData().split("\t")[0],
                   x[1].getUserData().split("\t")[1],
                   x[0].getUserData().split("\t")[0],
                   x[0].getUserData().split("\t")[2]]
        # x[1].geom]
    )
    # jim = mapped_rdd.take(10)
    # for row in jim:
    #     print(row)

    schema = t.StructType([t.StructField("gnaf_pid", t.StringType(), True),
                           t.StructField("state", t.StringType(), True),
                           t.StructField(bdy_id, t.StringType(), True),
                           t.StructField(bdy_id + "_state", t.StringType(), True)])
    # t.StructField('geom', GeometryType(), True)])

    join_df = spark.createDataFrame(mapped_rdd, schema)
    # join_df.printSchema()
    # join_df.show(10, False)

    export_to_parquet(join_df, "gnaf_with_{}_rdd_with_index".format(bdy_name))

    # num_joined_points = join_df.count()

    join_df.unpersist()
    mapped_rdd.unpersist()
    flat_mapped_rdd.unpersist()
    result_pair_rdd.unpersist()
    # bdy_rdd.unpersist()  # no method for SpatialRDD

    logger.info("\t - GNAF points boundary tagged with {}: {}"
                .format(bdy_name, datetime.now() - start_time))


def export_rdd(df, path_name, partition_and_index=None):

    # delete existing directory
    output_rdd_path = os.path.join(output_path, path_name)
    shutil.rmtree(output_rdd_path, True)

    output_rdd = Adapter.toSpatialRdd(df, "geom")
    output_rdd.analyze()

    # add partitioning and indexing to each partition and export RDD to disk
    if partition_and_index:
        output_rdd.spatialPartitioning(GridType.KDBTREE)
        output_rdd.buildIndex(IndexType.RTREE, True)  # needs to be set to False when writing to disk

        # rdd_with_other_attributes = output_rdd.rawSpatialRDD.map(lambda x: x.getUserData())
        # fred = rdd_with_other_attributes.take(10)
        # for row in fred:
        #     print(row)

        # output_rdd.indexedRawRDD.saveAsObjectFile(output_rdd_path)
    # else:

    # output_rdd.rawSpatialRDD.saveAsTextFile(output_rdd_path)

    return output_rdd


def export_bdys(spark, bdy_name, bdy_id, partition_and_index=None):
    # load boundaries
    sql = """SELECT {}, name, state, st_astext(geom) as wkt_geom
             FROM admin_bdys_202008.{}_analysis""".format(bdy_id, bdy_name)
    bdy_df = get_dataframe_from_postgres(spark, sql)

    # create geometries from WKT strings into new DataFrame
    bdy_df2 = bdy_df\
        .withColumn("geom", f.expr("st_geomFromWKT(wkt_geom)")) \
        .drop("wkt_geom")

    # write bdys to gzipped parquet
    return export_rdd(bdy_df2, bdy_name + "_rdd", partition_and_index)


def get_dataframe_from_postgres(spark, sql):
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", sql) \
        .option("properties", local_pg_settings["USER"]) \
        .option("password", local_pg_settings["PASS"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df
# .option("numPartitions", 32) \
# .option("partitionColumn", "gid") \


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
