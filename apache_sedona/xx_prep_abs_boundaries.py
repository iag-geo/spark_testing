
# script to load boundary & point data from Postgres and export it to parquet format for later use in a Spark script

import logging
import os
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark.sql import functions as f
from pyspark.sql import Window
from sedona.spark import *

# # REQUIRED FOR DEBUGGING IN IntelliJ/Pycharm ONLY - comment out if running from command line
# # set Conda environment vars for PySpark
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
# os.environ["SPARK_HOME"] = "/Users/hugh.saalmans/spark-2.4.6-bin-hadoop2.7"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# os.environ["PYSPARK_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/hugh.saalmans/opt/miniconda3/envs/sedona_env/bin/python"
# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"


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

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# number of CPUs to use in processing (defaults to number of local CPUs)
num_processors = cpu_count()


def main():
    start_time = datetime.now()

    # create spark session object
    config = (SedonaContext
              .builder()
              .master("local[*]")
              .appName("Sedona Test")
              .config("spark.sql.session.timeZone", "UTC")
              .config("spark.sql.debug.maxToStringFields", 100)
              .config("spark.sql.adaptive.enabled", "true")
              .config("spark.serializer", KryoSerializer.getName)
              .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
              .config("spark.executor.cores", 1)
              .config("spark.cores.max", num_processors)
              .config("spark.driver.memory", "4g")
              .config("spark.driver.maxResultSize", "2g")
              .getOrCreate()
              )

    # Add Sedona functions and types to Spark
    spark = SedonaContext.create(config)

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))
    start_time = datetime.now()

    # load ABS remoteness areas
    sql = """select ra_code_2021 as bdy_id, ra_name_2021 as bdy_type, state_name_2021 as state, st_astext(st_subdivide(geom, 512)) as wkt_geom
             from census_2021_bdys_gda94.ra_2021_aust_gda94
             where geom is not null"""
    ra_df = get_dataframe_from_postgres(spark, sql)

    # write remoteness areas to gzipped parquet
    export_to_parquet(ra_df, "boundaries")

    # load meshblock centroid coordinates (not geoms)
    sql = """select mb_code_2021 as point_id, state_name_2021 as state, st_y(st_centroid(geom)) as latitude, st_x(st_centroid(geom)) as longitude 
             from census_2021_bdys_gda94.mb_2021_aust_gda94
             where geom is not null"""
    mb_df = get_dataframe_from_postgres(spark, sql)

    # # filter to get every 4th row (to speed up the tutorial/demo code)
    # w = Window.orderBy(mb_df["point_id"])
    #
    # filtered_mb_df = mb_df.withColumn("row_number", f.row_number().over(w)) \
    #     .filter(f.col("row_number") % f.lit(4) == 0) \
    #     .drop("row_number")
    # filtered_mb_df.printSchema()
    # filtered_mb_df.show()
    # print(filtered_mb_df.count())

    # write meshblock coords to gzipped parquet
    export_to_parquet(mb_df, "points")
    # export_to_parquet(filtered_mb_df, "points")

    # cleanup
    spark.stop()

    logger.info("\t - Point and boundary data exported to gzipped parquet files: {}"
                .format(datetime.now() - start_time))


def get_dataframe_from_postgres(spark, sql):
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", sql) \
        .option("properties", local_pg_settings["USER"]) \
        .option("password", local_pg_settings["PASS"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df


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

    task_name = "Sedona testing"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
