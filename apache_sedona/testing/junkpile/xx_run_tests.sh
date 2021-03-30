#!/usr/bin/env bash

cd /Users/$(whoami)/git/iag_geo/spark_testing/apache_sedona/testing/junkpile

python 02_spatial_join_rdd_st_subdivide_benchmark.py
python 02_spatial_join_sql_st_subdivide_benchmark.py
