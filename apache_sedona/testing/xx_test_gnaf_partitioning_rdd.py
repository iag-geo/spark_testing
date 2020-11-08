
# script to load gnaf points & psma boundaries from Postgres to test Apache Sedona spatial join performance

import glob
import logging
import os
import psycopg2
import shutil
import sys

from datetime import datetime
from itertools import repeat
from multiprocessing import Pool, cpu_count
from psycopg2 import pool

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

# # REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# os.environ["JAVA_HOME"]="/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"]="/Users/hugh.saalmans/spark-3.0.1-bin-hadoop3.2"
# os.environ["SPARK_LOCAL_IP"]="127.0.0.1"
# os.environ["SPARK_LOCAL_DIRS"]="/Users/hugh.saalmans/tmp/spark"
# os.environ["PYSPARK_PYTHON"]="/Users/hugh.saalmans/opt/miniconda3/envs/geospark3_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/Users/hugh.saalmans/opt/miniconda3/envs/geospark3_env/bin/python"
# os.environ["PYLIB"]="${SPARK_HOME_DIR}/python/lib"

# set number of parallel processes (sets number of Spark executors and Postgres concurrent connections)
num_processors = cpu_count()

# set Postgres parameters
pg_settings = {'HOST': 'localhost', 'DB': 'geo', 'PORT': '5432', 'USER': 'postgres', 'PASS': 'password'}

# create Postgres JDBC url
jdbc_url = "jdbc:postgresql://{HOST}:{PORT}/{DB}".format(**pg_settings)

# get connect string for psycopg2
local_pg_connect_string = "dbname={DB} host={HOST} port={PORT} user={USER} password={PASS}".format(**pg_settings)

# create Postgres connection pool
pg_pool = psycopg2.pool.SimpleConnectionPool(1, num_processors, local_pg_connect_string)

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# GNAF csv file
gnaf_csv_file_path = os.path.join(output_path, "gnaf_light.csv")

# list of input boundary Postgres tables
bdy_list = [{"name": "commonwealth_electorates", "id": "ce_pid"},
            {"name": "local_government_areas", "id": "lga_pid"},
            {"name": "local_government_wards", "id": "ward_pid"},
            {"name": "state_lower_house_electorates", "id": "se_lower_pid"},
            {"name": "state_upper_house_electorates", "id": "se_upper_pid"}]


def main():
    start_time = datetime.now()

    # ----------------------------------------------------------
    # copy gnaf tables from Postgres to a CSV file - a one off
    #   - export required fields only and no header
    # ----------------------------------------------------------

    pg_conn = pg_pool.getconn()
    pg_cur = pg_conn.cursor()

    sql = """COPY (
                 SELECT longitude, latitude, gnaf_pid, state
                 FROM gnaf_202008.{}
             ) TO STDOUT WITH CSV"""

    # address principals
    with open(gnaf_csv_file_path, 'w') as csv_file:
        pg_cur.copy_expert(sql.format("address_principals"), csv_file)

    # append address aliases
    with open(gnaf_csv_file_path, 'a') as csv_file:
        pg_cur.copy_expert(sql.format("address_aliases"), csv_file)

    pg_cur.close()
    pg_pool.putconn(pg_conn)

    logger.info("\t - GNAF points exported to CSV: {}".format(datetime.now() - start_time))
    start_time = datetime.now()

    # ----------------------------------------------------------
    # create Spark session and context
    # ----------------------------------------------------------

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

    sc = spark.sparkContext

    logger.info("\t - PySpark {} session initiated: {}".format(sc.version, datetime.now() - start_time))
    start_time = datetime.now()

    # ----------------------------------------------------------
    # create GNAF PointRDD from CSV file
    # ----------------------------------------------------------

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

    logger.info("\t - Partitioned & Indexed GNAF RDD created: {}".format(datetime.now() - start_time))

    # ----------------------------------------------------------
    # get boundary tags using a spatial join
    # ----------------------------------------------------------

    for bdy in bdy_list:
        bdy_tag(spark, point_rdd, bdy)

    # point_rdd.unpersist()  # no such method on a SpatialRDD

    # ----------------------------------------------------------
    # merge boundary tag dataframes with GNAF records
    #   - required because spatial joins are INNER JOIN only,
    #     need to add untagged GNAF points
    # ----------------------------------------------------------

    start_time = datetime.now()

    # create gnaf dataframe and SQL view
    gnaf_df = spark.read \
        .option("header", False) \
        .option("inferSchema", True) \
        .csv(gnaf_csv_file_path) \
        .withColumnRenamed("_C0", "longitude") \
        .withColumnRenamed("_C1", "latitude") \
        .withColumnRenamed("_C2", "gnaf_pid") \
        .withColumnRenamed("_C3", "state")
    # gnaf_df.printSchema()
    # gnaf_df.show(10, False)

    gnaf_df.createOrReplaceTempView("pnt")

    # add bdy tags, one bdy type at a time
    for bdy in bdy_list:
        gnaf_df = join_bdy_tags(spark, bdy)
        gnaf_df.createOrReplaceTempView("pnt")

    # add point geoms for output to Postgres - in the PostGIS specific EWKT format
    final_df = gnaf_df.withColumn("geom", f.expr("concat('SRID=4326;POINT (', longitude, ' ', latitude, ')')")) \
        .drop("longitude") \
        .drop("latitude")
    # final_df.printSchema()
    # final_df.show(10, False)

    logger.info("\t - Boundary tags merged: {}".format(datetime.now() - start_time))

    # output result to Postgres
    export_to_postgres(final_df, "testing2.gnaf_with_bdy_tags",
                       os.path.join(output_path, "temp_gnaf_with_bdy_tags"), True)

    # cleanup
    spark.stop()

    # # optionally delete intermediate bdy tag files and GNAF csv file
    # for bdy in bdy_list:
    #     shutil.rmtree(os.path.join(output_path, "gnaf_with_{}".format(bdy["name"])))
    #
    # os.remove(gnaf_csv_file_path)


# add boundary tags to a copy of gnaf points
def join_bdy_tags(spark, bdy):

    # open bdy df
    bdy_tag_df = spark.read.parquet(os.path.join(output_path, "gnaf_with_{}".format(bdy["name"])))
    bdy_tag_df.createOrReplaceTempView("bdy_tag")

    sql = """SELECT pnt.*,
                        bdy_tag.{},
                        bdy_tag.{}
                 FROM pnt
                     LEFT OUTER JOIN bdy_tag ON pnt.gnaf_pid = bdy_tag.gnaf_pid""" \
        .format(bdy["id"], bdy["id"].replace("_pid", "_state"))
    join_df = spark.sql(sql)

    bdy_tag_df.unpersist()

    return join_df


# boundary tag gnaf points and save to a Parquet dataframe on disk
def bdy_tag(spark, point_rdd, bdy):
    start_time = datetime.now()

    # load boundaries
    bdy_rdd = get_bdy_rdd(spark, bdy)
    bdy_rdd.analyze()

    bdy_rdd.spatialPartitioning(point_rdd.getPartitioner())
    bdy_rdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    # run the join - returns a PairRDD with 1 boundary to 1-N points
    # e.g. [Geometry: Polygon userData: WA32       TANGNEY WA, [Geometry: Point userData: GAWA_146792426	WA, ...]]
    result_pair_rdd = JoinQuery.SpatialJoinQuery(point_rdd, bdy_rdd, True, True)
    # print(result_pair_rdd.take(1))

    # flat map values to have one point to bdy matched pair
    flat_mapped_rdd = result_pair_rdd.flatMapValues(lambda x: x)

    # map values to create RDD row of gnaf & bdy IDs, plus state data
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

    # convert result to a dataframe of the following shema
    schema = t.StructType([t.StructField("gnaf_pid", t.StringType(), False),
                           t.StructField("state", t.StringType(), False),
                           t.StructField(bdy["id"], t.StringType(), False),
                           t.StructField(bdy["id"].replace("_pid", "_state"), t.StringType(), False)])
    # t.StructField('geom', GeometryType(), True)])

    join_df = spark.createDataFrame(mapped_rdd, schema)
    # join_df.printSchema()
    # join_df.show(10, False)

    # save result to disk
    export_to_parquet(join_df, "gnaf_with_{}".format(bdy["name"]))

    # num_joined_points = join_df.count()  # this can be an expensive operation

    # cleanup datasets in memory
    join_df.unpersist()
    mapped_rdd.unpersist()
    flat_mapped_rdd.unpersist()
    result_pair_rdd.unpersist()
    # bdy_rdd.unpersist()  # no method for SpatialRDD

    logger.info("\t - GNAF points boundary tagged with {}: {}"
                .format(bdy["name"], datetime.now() - start_time))


# load bdy table from Postgres and create SpatialRDD from it
def get_bdy_rdd(spark, bdy):
    # load boundaries from Postgres
    sql = """SELECT {}, name, state, st_astext(geom) as wkt_geom
             FROM admin_bdys_202008.{}_analysis""".format(bdy["id"], bdy["name"])
    bdy_df = get_dataframe_from_postgres(spark, sql)

    # create geometries from WKT strings into new DataFrame
    bdy_df2 = bdy_df\
        .withColumn("geom", f.expr("st_geomFromWKT(wkt_geom)")) \
        .drop("wkt_geom")

    # create rdd
    output_rdd = Adapter.toSpatialRdd(bdy_df2, "geom")
    output_rdd.analyze()

    bdy_df2.unpersist()
    bdy_df.unpersist()

    return output_rdd


# load a dataframe from a Postgres query
def get_dataframe_from_postgres(spark, sql):
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", sql) \
        .option("properties", pg_settings["USER"]) \
        .option("password", pg_settings["PASS"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df


# export a dataframe to Parquet files
def export_to_parquet(df, name):
    df.write.option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(os.path.join(output_path, name))


# export a DataFrame to Postgres (via CSV files saved to disk)
def export_to_postgres(df, table_name, csv_folder, delete_files, partition_column=None):
    start_time = datetime.now()

    # get Postgres connection & cursor
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # # potentially expensive way to get number of DataFrame partitions!
    # num_partitions = df.rdd.getNumPartitions()

    # write to csv files - one gets written per partition
    # quotes are automatically put around strings with commas - i.e. csv is a safe export format
    if partition_column is not None:
        df.write.partitionBy(partition_column).csv(csv_folder, mode="overwrite", header=False, emptyValue="")
    else:
        df.write.csv(csv_folder, mode="overwrite", header=False, emptyValue="")

    # logger.info("exported dataframe to {:,} CSV files : {}"
    #     .format(num_partitions, datetime.now() - start_time))
    logger.info("\t - exported DataFrame to CSV files : {}".format(datetime.now() - start_time))
    start_time = datetime.now()

    # create table
    field_list = list()

    for bdy in bdy_list:
        field_list.append(bdy["id"] + " text")
        field_list.append(bdy["id"].replace("_pid", "_state") + " text")

    sql = """DROP TABLE IF EXISTS {0};
             CREATE TABLE {0}
             (
                 gnaf_pid text NOT NULL,
                 state text NOT NULL,
                 {1},
                 geom geometry(Point, 4326, 2) NOT NULL
             );
             ALTER TABLE {0} OWNER to postgres""".format(table_name, ",".join(field_list))

    pg_cur.execute(sql)
    # pg_cur.execute("TRUNCATE TABLE {}".format(table_name))

    # get all CSV file paths and copy CSV files to Postgres using multiprocessing
    file_list = list()

    if partition_column is not None:
        search_path = "{}/*/*/*.csv".format(csv_folder)
    else:
        search_path = "{}/*.csv".format(csv_folder)

    for file_name in glob.glob(search_path):
        file_list.append(file_name)

    with Pool(num_processors) as p:
        p.starmap(execute_copy, zip(file_list, repeat(table_name)))

    pg_cur.execute("ANALYSE {}".format(table_name))

    # delete CSV files
    if delete_files:
        shutil.rmtree(csv_folder)

    logger.info("\t - exported CSV files to Postgres : {}".format(datetime.now() - start_time))


def execute_copy(file_name, table_name):
    # get postgres connection from pool
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # Use a SQL statement. The Psycopg2 copy_from function has issues with quotes in CSV files
    sql = """COPY {}
             FROM '{}'
             WITH (DELIMITER ',', FORMAT CSV, NULL '')""". format(table_name, file_name)
    pg_cur.execute(sql)

    pg_cur.close()
    pg_pool.putconn(pg_conn)


if __name__ == "__main__":
    full_start_time = datetime.now()

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

    task_name = "Apache Sedona testing"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
