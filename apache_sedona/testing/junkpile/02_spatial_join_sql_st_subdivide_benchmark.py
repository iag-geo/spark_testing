
# script to benchmark spatial join performance between gnaf and a national boundary dataset

import os

from datetime import datetime
from multiprocessing import cpu_count
# from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

num_processors = cpu_count()
num_partitions = 162

# input path for gzipped parquet files
input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data")

# boundary info
bdy_name = "commonwealth_electorates"
bdy_id = "ce_pid"

# bdy table subdivision vertex limit
# max_vertices = [64, 128, 256, 512, 1024]
max_vertices = [256, 512]

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

# load gnaf points and create geoms
point_df = (spark.read.parquet(os.path.join(input_path, "address_principals"))
            # .select("gnaf_pid", "state", f.expr("ST_GeomFromWKT(wkt_geom)").alias("geom"))
            # .limit(1000000)
            .repartition(num_partitions, "state")
            )
point_df.createOrReplaceTempView("pnt")

for max_vertex in max_vertices:
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

    print("{:,} GNAF records boundary tagged with {} : {}"
          .format(join_df.count(), bdy_vertex_name, datetime.now() - start_time))

    join_df.unpersist()
    bdy_df.unpersist()

# cleanup
spark.stop()
