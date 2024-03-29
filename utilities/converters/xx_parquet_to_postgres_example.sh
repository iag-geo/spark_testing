#!/usr/bin/env bash

conda activate sedona

# test export from AWS S3 to local postgres (ABS Meshblock 2016 boundaries)
python3 /Users/$(whoami)/git/iag_geo/spark_testing/utilities/converters/parquet_to_postgres.py \
--aws-profile default \
--source-s3-bucket minus34.com \
--source-s3-folder opendata/geoscape-202105/parquet/abs_2016_mb \
--geom-field wkt_geom \
--geom-type POLYGON \
--srid 4283 \
--target-table testing.abs_2016_mb
