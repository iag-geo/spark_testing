
# script to download and load a remote GeoParquet file for when Apache Sedona supports the emerging GeoParquet format
#
# NOTE: as of 20220520 - geometry field currently loads as binary type; should be geometry type when supported
#

import logging
import os
import sys

from datetime import datetime
from multiprocessing import cpu_count

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# input path for parquet file
input_url = "https://storage.googleapis.com/open-geodata/linz-examples/nz-buildings-outlines.parquet"

# number of CPUs to use in processing (defaults to number of local CPUs)
num_processors = cpu_count()


def main():
    start_time = datetime.now()

    # create spark session object
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("Spatial Join Test")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.debug.maxToStringFields", 100)
             .config("spark.serializer", KryoSerializer.getName)
             .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.executor.cores", 1)
             .config("spark.cores.max", num_processors)
             .config("spark.driver.memory", "8g")
             .config("spark.driver.maxResultSize", "1g")
             .getOrCreate()
             )

    # Add Sedona functions and types to Spark
    SedonaRegistrator.registerAll(spark)

    logger.info(f"\t - PySpark {spark.sparkContext.version} session initiated: {datetime.now() - start_time}")
    start_time = datetime.now()

    # load sample file
    spark.sparkContext.addFile(input_url)

    logger.info(f"\t - Sample file downloaded: {datetime.now() - start_time}")
    start_time = datetime.now()

    df = spark.read.parquet(SparkFiles.get(os.path.basename(input_url)))
    num_rows = df.count()
    df.printSchema()
    df.show(5)

    logger.info(f"\t - created dataframe with {num_rows} rows: {datetime.now() - start_time}")

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

    task_name = "Apache Sedona testing"
    system_name = "mobility.ai"

    logger.info("{} started".format(task_name))
    logger.info("Running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    logger.info("{} finished : {}".format(task_name, time_taken))
    print()