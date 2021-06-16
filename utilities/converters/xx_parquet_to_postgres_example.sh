#!/usr/bin/env bash

# test export from AWS S3 to local postgres (ABS Meshblock 2016 boundaries)
python3 /Users/s57405/git/iag_geo/spark_testing/utilities/converters/parquet_to_postgres.py \
--aws-profile minus34 \
--source-s3-bucket minus34.com \
--source-s3-folder opendata/geoscape-202105/parquet/abs_2016_mb \
--geom-field ewkt_geom \
--geom-type MULIPOLYGON \
--srid 4283 \
--target-table testing.abs_2016_mb
