
# script to benchmark spatial join performance between gnaf and a national boundary dataset

import logging
import os

from datetime import datetime
from multiprocessing import cpu_count
# from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# setup logging - code is here to prevent conflict with logging.basicConfig() from one of the imports below
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

num_processors = cpu_count()

# input path for gzipped parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"

# bdy table subdivision vertex limit
max_vertices_list = [100, 200, 300, 400, 500]

# number of partitions on both dataframes
num_partitions_list = [50, 100, 150, 200, 250]

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

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
         .config("spark.executor.cores", 4)
         .config("spark.cores.max", num_processors)
         .config("spark.driver.memory", "8g")
         # .config("spark.driver.maxResultSize", "2g")
         .getOrCreate()
         )

# Add Sedona functions and types to Spark
SedonaRegistrator.registerAll(spark)

for num_partitions in num_partitions_list:

    # load gnaf points and create geoms
    point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
                # .select("gnaf_pid", "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
                # .limit(1000000)
                .repartition(num_partitions, "state")
                )
    point_df.createOrReplaceTempView("pnt")

    for max_vertex in max_vertices_list:
        start_time = datetime.now()

        bdy_vertex_name = "{}_{}".format(bdy_name, max_vertex)

        # load boundaries and create geoms
        bdy_df = (spark.read.parquet(os.path.join(input_path, bdy_vertex_name))
                  # .select(bdy_id, "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
                  .repartition(num_partitions, "state")
                  )
        bdy_df.createOrReplaceTempView("bdy")

        # run spatial join to boundary tag the points
        sql = """SELECT pnt.gnaf_pid, bdy.{} FROM pnt INNER JOIN bdy ON ST_Intersects(pnt.geom, bdy.geom)""".format(bdy_id)
        join_df = spark.sql(sql)

        logging.info("{:,} GNAF records boundary tagged with {} : {} partitions : {}"
              .format(join_df.count(), bdy_vertex_name, num_partitions, datetime.now() - start_time))

        join_df.unpersist()
        bdy_df.unpersist()

# cleanup
spark.stop()
