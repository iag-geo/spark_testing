
# script to test spatial joins between gnaf and select psma admin bdys - ~40 mins

import glob
import logging
import os
import psycopg2
import sys

from datetime import datetime
from itertools import repeat
from multiprocessing import cpu_count, Pool
from psycopg2 import pool

from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from geospark.register import upload_jars, GeoSparkRegistrator  # need to install geospark package
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator

# # REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# # set Conda environment vars for PySpark
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"] = "/Users/hugh.saalmans/spark-2.4.6-bin-hadoop2.7"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# os.environ["PYSPARK_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/geospark_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/geospark_env/bin/python"
# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"

num_processors = cpu_count() * 2


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

# # create postgres JDBC url
# jdbc_url = "jdbc:postgresql://{HOST}:{PORT}/{DB}".format(**local_pg_settings)

# get connect string for psycopg2
local_pg_connect_string = "dbname={DB} host={HOST} port={PORT} user={USER} password={PASS}".format(**local_pg_settings)

# create Postgres connection pool
pg_pool = psycopg2.pool.SimpleConnectionPool(1, num_processors, local_pg_connect_string)

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# gnaf csv file
input_file_name = os.path.join(output_path, "gnaf.csv")


def main():
    start_time = datetime.now()

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

    # Register Apache Sedona (geospark) UDTs and UDFs
    GeoSparkRegistrator.registerAll(spark)

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))
    start_time = datetime.now()

    # load gnaf points and create geoms
    # df = spark.read \
    #     .option("header", True) \
    #     .option("inferSchema", True) \
    #     .csv(input_file_name)
    #
    # point_df = df \
    #     .withColumn("geom", f.expr("ST_Point(longitude, latitude)")) \
    #     .cache()

    gnaf_df = spark.read.parquet(os.path.join(output_path, "gnaf"))
    point_df = gnaf_df.select("gnaf_pid", "state", "geom")\
        .repartitionByRange(100, "longitude")

    logger.info("\t - Loaded {:,} GNAF points: {}"
                .format(point_df.count(), datetime.now() - start_time))

    # boundary tag gnaf point
    tag_df = bdy_tag(spark, point_df, "commonwealth_electorates", "ce_pid")
    # tag_df.printSchema()

    tag_df2 = bdy_tag(spark, tag_df, "local_government_areas", "lga_pid")
    # tag_df2.printSchema()

    # bdy_tag(spark, "local_government_wards", "ward_pid")
    # bdy_tag(spark, "state_lower_house_electorates", "se_lower_pid")
    # bdy_tag(spark, "state_upper_house_electorates", "se_upper_pid")

    bdy_ids = "ce_pid text, lga_pid text"

    final_df = tag_df2.withColumn("wkt_geom", f.expr("concat('SRID=4326;POINT (', st_x(geom), ' ', st_y(geom), ')')"))\
        .drop("geom")
    # final_df.printSchema()

    # output to postgres, via CSV
    table_name = "gnaf_with_bdy_tags"
    export_to_postgres(final_df, "testing2.{}".format(table_name), bdy_ids, os.path.join(output_path, table_name))

    # cleanup
    spark.stop()


def bdy_tag(spark, point_df, bdy_name, bdy_id):
    start_time = datetime.now()

    point_df.createOrReplaceTempView("pnt")

    # load boundaries and create geoms
    bdy_df = spark.read.parquet(os.path.join(output_path, bdy_name)) \
        .withColumn("geom", f.expr("st_geomFromWKT(wkt_geom)"))
        # .repartitionByRange(100, "partition_id")
    bdy_df.createOrReplaceTempView("bdy")

    #         .withColumn("partition_id", f.percent_rank().over(Window.partitionBy().orderBy(f.expr("st_x(st_centroid(geom))"))) * f.lit(100.0)) \

    # run spatial join to boundary tag the points
    # notes:
    #   - spatial partitions and indexes for join will be created automatically
    #   - it's an inner join so point records could be lost (left joins not yet supported by Geospark)
    #   - force broadcast of unpartitioned boundaries (to speed up query) using /*+ BROADCAST(bdy) */
    sql = """SELECT /*+ BROADCAST(bdy) */ pnt.gnaf_pid,
                    bdy.{}
             FROM pnt
             INNER JOIN bdy ON ST_Intersects(pnt.geom, bdy.geom)""".format(bdy_id)
    # sql = """SELECT /*+ BROADCAST(bdy) */ pnt.gnaf_pid,
    #                 bdy.{},
    #                 pnt.geom
    #          FROM pnt
    #          INNER JOIN bdy ON pnt.partition_id = bdy.partition_id AND ST_Intersects(pnt.geom, bdy.geom)""".format(bdy_id)
    join_df = spark.sql(sql)
    join_df.createOrReplaceTempView("bdy_join")
    # join_df.explain()

    # get missing gnaf records due to no left join with a spatial join (above)
    sql = """SELECT pnt.*,
                    bdy_join.{}
             FROM pnt
             LEFT OUTER JOIN bdy_join ON pnt.gnaf_pid = bdy_join.gnaf_pid""".format(bdy_id)
    join_df2 = spark.sql(sql)

    # num_joined_points = join_df.count()

    # join_df.printSchema()
    # join_df.show(5)

    # output join DataFrame
    # export_to_parquet(join_df, "gnaf_with_{}".format(bdy_name))

    join_df.unpersist()
    bdy_df.unpersist()

    logger.info("\t - GNAF boundary tagged with {} : {}"
                .format(bdy_name, datetime.now() - start_time))

    return join_df2


def export_to_parquet(df, name):
    df.write.option("compression", "gzip") \
        .mode("overwrite") \
        .parquet(os.path.join(output_path, name))


def export_to_postgres(df, table_name, bdy_id, csv_folder, partition_column=None):
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
    logger.info("\t\t - exported DataFrame to CSV files : {}".format(datetime.now() - start_time))
    start_time = datetime.now()

    # create table (todo: not a prod grade way to treat your hard drive...)
    sql = """DROP TABLE IF EXISTS {0} CASCADE;
             CREATE TABLE {0} (
                 gnaf_pid text NOT NULL,
                 state text,
                 {1},
                 geom geometry(Point, 4326, 2) NOT NULL
             ) WITH (OIDS=FALSE);
             ALTER TABLE {0} OWNER TO postgres""".format(table_name, bdy_id)
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

    pg_cur.close()
    pg_pool.putconn(pg_conn)

    # logger.info("copied {:,} CSV files to {} : {}"
    #             .format(num_partitions, table_name, datetime.now() - start_time))
    logger.info("\t\t - imported CSV files to {} : {}"
                .format(table_name, datetime.now() - start_time))


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
