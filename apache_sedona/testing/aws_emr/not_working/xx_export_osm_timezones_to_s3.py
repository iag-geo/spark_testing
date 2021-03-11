# ---------------------------------------------------------------------------------------------------------------------
#
# script to import OSM Time Zones from Postgres & export to GZIPped Parquet files in AWS S3
#
# PROCESS
#
# 1. get table from Postgres
# 2. import into Spark dataframe
# 3. export as gzip parquet files to local disk
# 4. copy files to S3
#
# note: direct export from Spark to S3 isn't used to avoid Hadoop install and config
#
# ---------------------------------------------------------------------------------------------------------------------

import boto3
import logging
# import math
import os
import psycopg2
import sys

from boto3.s3.transfer import TransferConfig
from datetime import datetime
from multiprocessing import cpu_count

from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession

# setup logging - code is here to prevent conflict with logging.basicConfig() from one of the imports below
log_file = os.path.abspath(__file__).replace(".py", ".log")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p")

from sedona.register import upload_jars, SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator

# REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# home = "/Users/hugh.saalmans"
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"] = home + "/spark-3.0.1-with-sedona"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# os.environ["SPARK_LOCAL_DIRS"] = home + "/tmp/spark"
# os.environ["PYSPARK_PYTHON"] = home + "/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = home + "/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"

# set number of parallel processes (sets number of Spark executors and Postgres concurrent connections)
num_processors = cpu_count()
# num_processors = 6


# set postgres connection parameters
def get_password(connection_name):
    # hack to get credentials from local file
    passwords_file_path = os.path.join(os.environ["GIT_HOME"], "passwords.ini")

    if os.path.exists(passwords_file_path):
        passwords_file = open(passwords_file_path,'r').read().splitlines()
        passwords_file = [i for i in passwords_file if len(i) != 0]  # remove empty lines
        passwords_file = [i for i in passwords_file if i[0] != "#"]  # remove comment lines

        params = dict()
        for ini in passwords_file:
            params[ini.split()[0].rstrip().lstrip()] = ini.split(':=')[1].rstrip().lstrip()

        return dict(item.split("|") for item in params[connection_name].split(","))


pg_settings = get_password("localhost_super")

# create Postgres JDBC url
jdbc_url = "jdbc:postgresql://{HOST}:{PORT}/{DB}".format(**pg_settings)

# get connect string for psycopg2
pg_connect_string = "dbname={DB} host={HOST} port={PORT} user={USER} password={PASS}".format(**pg_settings)

# aws details
s3_bucket = "minus34.com"
s3_folder = "osm"

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# table to export to S3
schema_name = "osm"
table_name = "time_zone"


def main():
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
             .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
             .config("spark.cores.max", num_processors)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.driver.memory", "8g")
             .getOrCreate()
             )

    #              .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # Register Apache Sedona UDTs and UDFs
    SedonaRegistrator.registerAll(spark)

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))

    # get list of tables to export to S3
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_cur = pg_conn.cursor()

    # --------------------------------------------------------------
    # import each table from each schema in Postgres &
    # export to GZIPped Parquet files in AWS S3
    # --------------------------------------------------------------

    start_time = datetime.now()

    # check what type of geometry field the table has and what it's coordinate system is
    sql = """select f_geometry_column, type, srid from public.geometry_columns
             where f_table_schema = '{}'
                 and f_table_name = '{}'""".format(schema_name, table_name)
    pg_cur.execute(sql)
    result = pg_cur.fetchone()

    if result is not None:
        geom_field = result[0]
        geom_type = result[1]
        geom_srid = result[2]
    else:
        geom_field = None
        geom_type = None
        geom_srid = None

    # build geom field sql
    # note: exported geom field will be WGS84 (EPSG:4326) Well Known Binaries (WKB)
    if geom_field is not None:
        if "POLYGON" in geom_type or "LINESTRING" in geom_type:
            geom_sql = ",ST_AsText(ST_Subdivide((ST_Dump(ST_Buffer(geom, 0.0))).geom, 512)) as wkt_geom"
        else:
            geom_sql = ",ST_AsText(geom) as wkt_geom"

        # transform geom to WGS84 if required
        if geom_srid != 4326:
            geom_sql = geom_sql.replace("(geom", "(ST_Transform(geom, 4326)")

    else:
        geom_sql = ""

    # build query to select all columns and the WKB geom if exists
    sql = """SELECT 'SELECT ' || array_to_string(ARRAY(
                 SELECT column_name
                 FROM information_schema.columns
                 WHERE table_name = '{1}'
                     AND table_schema = '{0}'
                     AND column_name NOT IN('geom')
             ), ',') || '{2} ' ||
                    'FROM {0}.{1}' AS sqlstmt"""\
        .format(schema_name, table_name, geom_sql)
    pg_cur.execute(sql)
    query = str(pg_cur.fetchone()[0])  # str is just there for intellisense in Pycharm

    # add filter to query
    query = query + " WHERE tzid LIKE 'Australia%'"
    # print(query)

    bdy_df = import_bdys(spark, query)
    export_to_parquet(bdy_df, table_name)
    copy_to_s3(schema_name, table_name)

    bdy_df.unpersist()

    logger.info("\t\t exported {} : {}".format(table_name, datetime.now() - start_time))

    # cleanup
    pg_cur.close()
    pg_conn.close()
    spark.stop()


# load bdy table from Postgres and create a geospatial dataframe from it
def import_bdys(spark, sql):

    # load boundaries from Postgres
    df = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", "({}) as sqt".format(sql))
          .option("properties", pg_settings["USER"])
          .option("password", pg_settings["PASS"])
          .option("driver", "org.postgresql.Driver")
          .option("fetchSize", 1000)
          .load()
          )

    # create geometries from WKT strings into new DataFrame
    df2 = df.withColumn("geom", f.expr("st_geomFromWKT(wkt_geom)")) \
        .drop("wkt_geom")

    return df2


# export a dataframe to gz parquet files
def export_to_parquet(df, name):
    df.write.option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(os.path.join(output_path, name))


def copy_to_s3(schema_name, name):

    # delete existing files (each time you run this Spark creates new, random parquet file names)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    bucket.objects.filter(Prefix=os.path.join(s3_folder, name)).delete()

    s3_client = boto3.client('s3')
    config = TransferConfig(multipart_threshold=1024 ** 2)  # 1MB

    # upload one file at a time
    for root,dirs,files in os.walk(os.path.join(output_path, name)):
        for file in files:
            response = s3_client\
                .upload_file(os.path.join(output_path, name, file), s3_bucket, os.path.join(s3_folder, name, file)
                             , Config=config)

            if response is not None:
                logger.warning("\t\t\t - {} copy to S3 problem : {}".format(name, response))


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

    task_name = "OSM Time Zone Boundary Export to S3"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
