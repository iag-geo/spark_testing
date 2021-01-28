
# script to compare a day, a month and a year of GDELT data using Spark and AWS EMR

import boto3
import logging
import os
import sys

from datetime import datetime

from pyspark.sql import functions as f, types as t  # need to install pyspark package
from pyspark.sql import SparkSession, Window

from sedona.register import SedonaRegistrator  # need to install geospark package
from sedona.utils import KryoSerializer, SedonaKryoRegistrator

# # target records per partition
# records_per_partition = 2500000

# S3 paths - must use the s3:// prefix for S3 files when using EMRFS
input_s3_path = "s3a://gdelt-open-data/events/"

input_day_path = input_s3_path + "20180823.export.csv"
input_month_path = input_s3_path + "201808*.export.csv"
input_year_path = input_s3_path + "2018*.export.csv"


def main():
    start_time = datetime.now()

    # get AWS creds (for running Spark outside of AWS EMR)
    session = boto3.Session()
    credentials = session.get_credentials()
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key
    # aws_session_token = credentials.token

    spark = (SparkSession
             .builder
             .appName("gdelt_testing")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             # .config("spark.hadoop.fs.s3.fast.upload", "true")
             # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
             # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
             .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
             .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
             .config("spark.driver.maxResultSize", "0")
             .getOrCreate()
             )

    # Register Apache Sedona (geospark) UDTs and UDFs
    SedonaRegistrator.registerAll(spark)

    sc = spark.sparkContext

    logger.info("{} initiated on PySpark {} : {}"
                .format(sc.applicationId, sc.version, datetime.now() - start_time))
    logger.info("\t - Running on Python {}".format(sys.version.replace("\n", " ")))
    start_time = datetime.now()

    # load day dataframe and get stats
    day_df = spark.read.option("inferSchema", "true").csv(input_day_path)
    day_df.printSchema()
    day_df.show(5)

    # release the dataframes' memory
    day_df.unpersist()
    day_df.printSchema()






    # month_df = spark.read.parquet(input_month_path)
    #
    # year_df = spark.read.parquet(input_year_path)




    # cleanup
    spark.stop()


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

    task_name = "GDELT Data Testing"
    system_name = "IAG Geo"

    logger.info("{} started : {}".format(task_name, datetime.now()))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()
