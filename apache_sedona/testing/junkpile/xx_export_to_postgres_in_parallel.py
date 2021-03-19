
# script to export a Spark dataframe to Postgres in parallel - without needing to know the table schema
#
# script uses Pandas to create table without having to know the data schema
#
# data import to Postgres done in parallel using partitioned CSV files (many times faster than Pandas with >1m rows)
#

import glob
import logging
import os
import pandas
import psycopg2
import shutil
import sqlalchemy
import sys

from datetime import datetime
from itertools import repeat
from multiprocessing import Pool, cpu_count

from psycopg2 import pool

from pyspark.sql import functions as f
from pyspark.sql import SparkSession

# from sedona.register import SedonaRegistrator
# from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# task name for logging
task_name = "Export to Postgres"

# how many CPUs to use in Spark and CSV import to Postgres
num_processors = cpu_count()

# create Postgres connection pool
local_pg_connect_string = "dbname=geo host=localhost port=5432 user=postgres password=pssword"
pg_pool = psycopg2.pool.SimpleConnectionPool(1, num_processors + 1, local_pg_connect_string)

# postgres connect string for SQL Alchemy (script uses Pandas to create table without having to know the data schema)
sql_alchemy_engine_string = "postgresql+psycopg2://postgres:password@localhost/geo"

# target Postgres schema
schema_name = "testing"

# output path for temporary CSV files
temp_folder = "/Users/hugh.saalmans/tmp/spark"

# settings for Spark dataframe
input_file_path = "/path/to/my/data/*.csv.gz"
output_table_name = "my_table_name"


def main():
    start_time = datetime.now()

    # create SQLalchemy engine
    sql_engine = sqlalchemy.create_engine(sql_alchemy_engine_string)

    # create spark session object
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("Export to Postgres")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             # .config("spark.serializer", KryoSerializer.getName)
             # .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
             .config("spark.kryoserializer.buffer.max", "512m")
             # .config("spark.jars.packages",
             #         "org.apache.spark:spark-avro_2.12:3.0.2,"
             #         "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,"
             #         "org.datasyslab:geotools-wrapper:geotools-24.0")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             # .config("spark.executor.cores", 4)
             .config("spark.cores.max", num_processors)
             .config("spark.driver.memory", "8g")
             .config("spark.driver.maxResultSize", "2g")
             .getOrCreate()
             )

    # # Add Sedona functions and types to Spark
    # SedonaRegistrator.registerAll(spark)

    logger.info("\t - PySpark {} session initiated: {}".format(spark.sparkContext.version, datetime.now() - start_time))
    start_time = datetime.now()

    # ----------------------------------------------------------------------------------------------------------------
    #  Create dataframe from input files
    # ----------------------------------------------------------------------------------------------------------------

    # import files into dataframe
    df = spark.read.csv(input_file_path, inferSchema=True, header=True)
    # df = spark.read.format("avro").load(input_file_path)
    # df.printSchema()
    # df.show(10, False)

    # OPTIONAL - do some ETL on the dataframe
    # output_df = etc.....

    # ----------------------------------------------------------------------------------------------------------------
    #  Export dataframe to Postgres
    # ----------------------------------------------------------------------------------------------------------------

    # export to Postgres
    export_to_postgres(df, 100, output_table_name, sql_engine)
    df.unpersist()

    logger.info("\t - {} exported to Postgres: {}".format(output_table_name, datetime.now() - start_time))
    start_time = datetime.now()

    # cleanup
    spark.stop()

    logger.info("\t - Spark stopped : {}".format(datetime.now() - start_time))


# export to Postgres using Pandas to define the table schema
# TODO: add support for geometry fields
def export_to_postgres(df, num_partitions, table_name, sql_engine):
    start_time = datetime.now()

    # output folder for temp CSVs
    csv_folder = os.path.join(temp_folder, "temp_{}".format(table_name))

    # get Postgres connection & cursor
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # ----------------------------------------------------------------------------------------------------------------
    # STEP 1 - create table in Postgres
    # ----------------------------------------------------------------------------------------------------------------

    # get first row in dataframe and create table in Postgres with it
    pandas_df = df.limit(1).toPandas()
    pandas_df.to_sql(table_name, sql_engine, schema=schema_name, index=False, if_exists="replace")

    # delete first row and vacuum table - DBAs be like "what the!"
    pg_cur.execute("DELETE FROM {}.{}".format(schema_name, table_name))
    pg_cur.execute("VACUUM FULL {}.{}".format(schema_name, table_name))

    # ----------------------------------------------------------------------------------------------------------------
    # STEP 2 - export dataframe to partitioned CSVs
    # ----------------------------------------------------------------------------------------------------------------

    # write to csv files
    # note: Spark puts quotes around strings with commas - i.e. CSV is a safe export format
    df.repartition(num_partitions).write \
        .csv(csv_folder, mode="overwrite", header=False, emptyValue="")

    logger.info("\t\t - exported {} to CSV files : {}".format(table_name, datetime.now() - start_time))
    start_time = datetime.now()

    # ----------------------------------------------------------------------------------------------------------------
    # STEP 3 - import into Postgres in parallel
    # ----------------------------------------------------------------------------------------------------------------

    # get exported CSV file list
    file_list = list()
    search_path = "{}/*.csv".format(csv_folder)
    for file_name in glob.glob(search_path):
        file_list.append(file_name)

    # import CSVs in parallel
    with Pool(num_processors) as p:
        p.starmap(execute_copy, zip(file_list, repeat(schema_name), repeat(table_name)))

    # update table stats
    pg_cur.execute("ANALYSE {}.{}".format(schema_name, table_name))

    # delete CSV files
    shutil.rmtree(csv_folder)

    # return Postgres connection to pool
    pg_cur.close()
    pg_pool.putconn(pg_conn)

    logger.info("\t\t - exported {} CSV files to Postgres : {}".format(table_name, datetime.now() - start_time))


def execute_copy(file_name, schema_name, table_name):
    # get postgres connection from pool
    pg_conn = pg_pool.getconn()
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # Use a SQL statement. The Psycopg2 copy_from function has issues with quotes in CSV files
    sql = """COPY {}.{} FROM '{}' WITH (DELIMITER ',', FORMAT CSV, NULL '')""" \
        .format(schema_name, table_name, file_name)
    pg_cur.execute(sql)

    pg_cur.close()
    pg_pool.putconn(pg_conn)


if __name__ == "__main__":
    full_start_time = datetime.now()

    # setup logging
    log_file = os.path.abspath(__file__).replace(".py", ".log")
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(asctime)s %(message)s",
                        datefmt="%m/%d/%Y %I:%M:%S %p")

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

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
