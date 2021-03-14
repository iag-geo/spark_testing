# ---------------------------------------------------------------------------------------------------------------------
#
# script to import each GNAF & administrative boundary table from Postgres & export as GZIPped Parquet files to AWS S3
#
# PROCESS
#
# 1. get list of tables from Postgres
# 2. for each table:
#     a. import into Spark dataframe
#     b. export as gzip parquet files to local disk
#     c. copy files to S3
#
# note: direct export from Spark to S3 isn't used to avoid Hadoop install and config
#
# ---------------------------------------------------------------------------------------------------------------------

import logging
import math
import os
import psycopg2
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark.sql import SparkSession, functions as f
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# setup logging - code is here to prevent conflict with logging.basicConfig() from one of the imports below
log_file = os.path.abspath(__file__).replace(".py", ".log")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p")

# set number of parallel processes (sets number of Spark executors and concurrent Postgres JDBC connections)
num_processors = cpu_count()


# set postgres connection parameters
def get_password(connection_name):
    # get credentials from local file
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

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# input GNAF table
points_schema = "gnaf_202102"
points_table = "address_principals"
points_id = "gnaf_pid"

# input boundary table
bdy_schema = "admin_bdys_202102"
bdy_table = "commonwealth_electorates"
bdy_id = "ce_pid"

# bdy table subdivision vertex limit
max_vertices = [64, 128, 256, 512, 1024]


def main():
    start_time = datetime.now()

    # ----------------------------------------------------------
    # create Spark session and context
    # ----------------------------------------------------------

    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("query")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.cores.max", num_processors)
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
             # .config("spark.jars.packages",
             #         'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,'
             #         'org.datasyslab:geotools-wrapper:geotools-24.0')
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.driver.memory", "8g")
             .getOrCreate()
             )

    # Add Sedona functions and types to Spark
    SedonaRegistrator.registerAll(spark)

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))

    # get list of tables to export to S3
    pg_conn = psycopg2.connect(pg_connect_string)
    pg_cur = pg_conn.cursor()

    # ----------------------------------------------------------------------------
    # import points table in Postgres & export to GZIPped Parquet local files
    # ----------------------------------------------------------------------------

    # get min and max gid values to enable parallel import from Postgres to Spark
    # add gid field based on row number if missing
    sql = """SELECT min(gid), max(gid) FROM {}.{}""".format(points_schema, points_table)
    pg_cur.execute(sql)
    gid_range = pg_cur.fetchone()
    min_gid = gid_range[0]
    max_gid = gid_range[1]

    sql = "SELECT gid, {}, ST_AsText(ST_Transform(geom, 4326)) as wkt_geom FROM {}.{}" \
        .format(points_id, points_schema, points_table)

    # import to Spark in parallel using JDBC
    points_df = import_table(spark, sql, min_gid, max_gid, 500000)

    # export to parquet on local drive after adding geometry field
    export_df = points_df.withColumn("geom", f.expr("ST_GeomFromWKT(wkt_geom)")) \
        .drop("wkt_geom")

    export_to_parquet(export_df, points_table)

    export_df.unpersist()
    points_df.unpersist()

    logger.info("\t - exported {} : {}".format(points_table, datetime.now() - start_time))
    start_time = datetime.now()

    # ----------------------------------------------------------------------------
    # import boundary table in Postgres & export to GZIPped Parquet local files
    # ----------------------------------------------------------------------------

    # get min and max gid values to enable parallel import from Postgres to Spark
    # add gid field based on row number if missing
    sql = """SELECT min(gid), max(gid) FROM {}.{}""".format(bdy_schema, bdy_table)
    pg_cur.execute(sql)
    gid_range = pg_cur.fetchone()
    min_gid = gid_range[0]
    max_gid = gid_range[1]

    for max_vertex in max_vertices:

        sql = """SELECT gid, {}, 
                     ST_AsText(ST_Subdivide((ST_Dump(ST_Buffer(ST_Transform(geom, 4326), 0.0))).geom, {})) as wkt_geom 
                     FROM {}.{}""" \
            .format(bdy_id, max_vertex, bdy_schema, bdy_table)

        # import to Spark in parallel using JDBC
        bdy_df = import_table(spark, sql, min_gid, max_gid, 5000)

        # export to parquet on local drive after adding geometry field
        export_df = bdy_df.withColumn("geom", f.expr("ST_GeomFromWKT(wkt_geom)")) \
            .drop("wkt_geom")

        export_name = "{}_{}".format(bdy_table, max_vertex)

        export_to_parquet(export_df, export_name)

        export_df.unpersist()
        bdy_df.unpersist()

        logger.info("\t - exported {}_{} : {}".format(bdy_table, export_name, datetime.now() - start_time))

    # cleanup
    pg_cur.close()
    pg_conn.close()
    spark.stop()


# load bdy table from Postgres and create a geospatial dataframe from it
def import_table(spark, sql, min_gid, max_gid, partition_size):

    # get the number of partitions
    num_partitions = math.ceil(float(max_gid - min_gid) / float(partition_size))

    # load boundaries from Postgres in parallel
    df = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", "({}) as sqt".format(sql))
          .option("properties", pg_settings["USER"])
          .option("password", pg_settings["PASS"])
          .option("driver", "org.postgresql.Driver")
          .option("fetchSize", 1000)
          .option("partitionColumn", "gid")
          .option("lowerBound", min_gid)
          .option("upperBound", max_gid)
          .option("numPartitions", num_partitions)
          .load()
          )

    return df


# export a dataframe to gz parquet files
def export_to_parquet(df, name):
    df.write.option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(os.path.join(output_path, name))


def copy_to_s3(schema_name, name):

    # set correct AWS user
    boto3.setup_default_session(profile_name="default")

    # delete existing files (each time you run this Spark creates new, random parquet file names)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    bucket.objects.filter(Prefix=os.path.join(s3_folder, schema_name, name)).delete()

    s3_client = boto3.client('s3')
    config = TransferConfig(multipart_threshold=1024 ** 2)  # 1MB

    # upload one file at a time
    for root,dirs,files in os.walk(os.path.join(output_path, name)):
        for file in files:
            response = s3_client\
                .upload_file(os.path.join(output_path, name, file), s3_bucket,
                             os.path.join(s3_folder, schema_name, name, file),
                             Config=config, ExtraArgs={'ACL': 'public-read'})

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

    task_name = "PSMA Admin Boundary Export to S3"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
