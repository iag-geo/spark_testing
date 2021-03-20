
# script to benchmark spatial join performance between gnaf and a national boundary dataset

import logging
import os
# import platform

from datetime import datetime
from multiprocessing import cpu_count
# from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

computer = "imac5"

# setup logging - code is here to prevent conflict with logging.basicConfig() from one of the imports below
log_file = os.path.abspath(__file__).replace(".py", ".csv")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format="%(message)s")

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

num_processors = cpu_count()

# input path for gzipped parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"

# bdy table subdivision vertex limit
max_vertices_list = [100, 150, 200, 250]

# number of partitions on both dataframes
# num_partitions_list = [150, 200, 250, 300, 350, 400, 450]
num_partitions_list = [500, 1000, 1500, 2000]

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# log header for log file (so results cn be used in Excel/Tableau)
logger.info("computer,points,boundaries,max_vertices,partitions,processing_time")

for num_partitions in num_partitions_list:
    for max_vertices in max_vertices_list:

        bdy_vertex_name = "{}_{}".format(bdy_name, max_vertices)

        # create spark session object
        spark = (SparkSession
                 .builder
                 .master("local[*]")
                 .appName("Spatial Join SQL Benchmark")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.debug.maxToStringFields", 100)
                 .config("spark.serializer", KryoSerializer.getName)
                 .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
                 # .config("spark.jars.packages",
                 #         'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,'
                 #         'org.datasyslab:geotools-wrapper:geotools-24.0')
                 .config("spark.sql.adaptive.enabled", "true")
                 # .config("spark.executor.cores", 4)
                 .config("spark.cores.max", num_processors)
                 .config("spark.driver.memory", "8g")
                 # .config("spark.driver.maxResultSize", "2g")
                 .getOrCreate()
                 )

        # Add Sedona functions and types to Spark
        SedonaRegistrator.registerAll(spark)

        start_time = datetime.now()

        # load gnaf points and create geoms
        point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
                    # .select("gnaf_pid", "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
                    # .limit(1000000)
                    .repartition(num_partitions, "state")
                    .cache()
                    )
        point_df.createOrReplaceTempView("pnt")

        # load boundaries and create geoms
        bdy_df = (spark.read.parquet(os.path.join(input_path, bdy_vertex_name))
                  # .select(bdy_id, "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
                  .repartition(num_partitions, "state")
                  .cache()
                  )
        bdy_df.createOrReplaceTempView("bdy")

        # run spatial join to boundary tag the points
        sql = """SELECT pnt.gnaf_pid, bdy.{} FROM pnt INNER JOIN bdy ON ST_Intersects(pnt.geom, bdy.geom)"""\
            .format(bdy_id)
        join_df = spark.sql(sql)

        # log stats
        logging.info("{},{},{},{},{},{}"
                     .format(computer, join_df.count(), bdy_df.count(), max_vertices, num_partitions, datetime.now() - start_time))

        join_df.unpersist()
        bdy_df.unpersist()

        # cleanup
        spark.stop()
