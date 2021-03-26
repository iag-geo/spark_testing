
# script to benchmark spatial join performance between gnaf and a national boundary dataset

import os

from datetime import datetime
from multiprocessing import cpu_count
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

start_time = datetime.now()

num_processors = cpu_count() * 2
num_partitions = num_processors * 24

# input path for gzipped parquet files
input_path = "/Users/hugh.saalmans/git/minus34/gnaf-loader/spark/data"

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"

# output path for gzipped parquet files
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data")

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
         .config("spark.driver.memory", "12g")
         # .config("spark.driver.maxResultSize", "2g")
         .getOrCreate()
         )

# Add Sedona functions and types to Spark
SedonaRegistrator.registerAll(spark)

# load gnaf points and create geoms
point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
            .select("gnaf_pid", "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
            # .limit(1000000)
            .repartition(num_partitions, "state")
            )
point_df.createOrReplaceTempView("pnt")

# load boundaries and create geoms
bdy_df = (spark.read.parquet(os.path.join(input_path, bdy_name))
          .select(bdy_id, "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
          .repartition(num_partitions, "state")
          )
bdy_df.createOrReplaceTempView("bdy")

# run spatial join to boundary tag the points
sql = """SELECT pnt.gnaf_pid, bdy.{} FROM pnt INNER JOIN bdy ON ST_Intersects(pnt.geom, bdy.geom)""".format(bdy_id)
join_df = spark.sql(sql)

print("{:,} GNAF records boundary tagged with {} : {}".format(join_df.count(), bdy_name, datetime.now() - start_time))

# cleanup
spark.stop()
