#!/usr/bin/env bash

SECONDS=0*

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"

# --------------------------------------------------------------------------------------------------------------------
# Script installs Apache Sedona (formerly Geospark) locally on a Mac with Spark in standalone mode
# --------------------------------------------------------------------------------------------------------------------
#
# Author: Hugh Saalmans, IAG Strategy & Innovation
# Date: 2020-09-25
#
# WARNINGS:
#   - Removes existing 'sedona' Conda environment
#
# PRE_REQUISITES:
#   1. Java 11 OpenJDK is installed using Homebrew: brew install openjdk@11
#
#   2. Miniconda is installed in the default directory ($HOME/opt/miniconda3)
#        - Mac Intel installer: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg
#
# ISSUES:
#   1. Conda environment variables aren't accessible in IntelliJ/Pycharm due to a missing feature
#        - Sedona Python scripts will fail in IntelliJ/Pycharm as Spark env vars aren't set (e.g. $SPARK_HOME)
#
# --------------------------------------------------------------------------------------------------------------------
#
# SETUP:
#   - edit these if it's now the future and versions have changed
#

PYTHON_VERSION="3.9"
SEDONA_VERSION="1.1.0"
GEOTOOLS_VERSION="24.1"
POSTGRES_JDBC_VERSION="42.2.23"

# --------------------------------------------------------------------------------------------------------------------

# get the directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# this assumes miniconda is installed in the default directory on linux/MacOS
SPARK_HOME_DIR="${HOME}/opt/miniconda3/envs/sedona/lib/python${PYTHON_VERSION}/site-packages/pyspark"

cd ${HOME}

echo "-------------------------------------------------------------------------"
echo "Creating new Conda Environment 'sedona'"
echo "-------------------------------------------------------------------------"

# stop the current Conda environment (if any running)
conda deactivate

# WARNING - remove existing environment
conda env remove --name sedona

# update Conda platform
conda update -y conda

# create Conda environment
conda create -y -n sedona python=${PYTHON_VERSION}

# activate env
conda activate sedona

# add environment variables for Pyspark
conda env config vars set JAVA_HOME="/usr/local/opt/openjdk@11"
conda env config vars set SPARK_HOME="${SPARK_HOME_DIR}"
conda env config vars set SPARK_LOCAL_IP="127.0.0.1"
conda env config vars set SPARK_LOCAL_DIRS="${HOME}/tmp/spark"
conda env config vars set PYSPARK_PYTHON="${HOME}/opt/miniconda3/envs/sedona/bin/python${PYTHON_VERSION}"
conda env config vars set PYSPARK_DRIVER_PYTHON="${HOME}/opt/miniconda3/envs/sedona/bin/ipython3"
conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"

# reactivate for env vars to take effect
conda activate sedona

# install supporting & useful packages
conda install -y -c conda-forge psycopg2 sqlalchemy geoalchemy2 geopandas pyarrow jupyter matplotlib

## OPTIONAL - AWS Packages
#conda install -y -c conda-forge boto3 s3fs

echo "-------------------------------------------------------------------------"
echo "Install Pyspark with Apache Sedona"
echo "-------------------------------------------------------------------------"

# includes full ApacheSpark install
pip install apache-sedona[spark]

# create folder for Spark temp files
mkdir -p ${HOME}/tmp/spark

echo "-------------------------------------------------------------------------"
echo "Downloading additional JAR files"
echo "-------------------------------------------------------------------------"

cd ${SPARK_HOME_DIR}/jars

# add Apache Sedona Python shaded JAR and GeoTools
curl -O https://repo1.maven.org/maven2/org/apache/sedona/sedona-python-adapter-3.0_2.12/${SEDONA_VERSION}-incubating/sedona-python-adapter-3.0_2.12-${SEDONA_VERSION}-incubating.jar
curl -O https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${SEDONA_VERSION}-${GEOTOOLS_VERSION}/geotools-wrapper-${SEDONA_VERSION}-${GEOTOOLS_VERSION}.jar

# add Postgres JDBC driver to Spark (optional - included for running xx_prep_abs_boundaries.py)
curl -O https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

# get hadoop-aws & aws-java-sdk JAR files (optional - required for accessing AWS S3)
#curl -O https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
#curl -O https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk/1.11.880/aws-java-sdk-1.11.880.jar

# get Google Storage connector shaded JAR (optional - required for accessing GCP Storage)
#curl -O https://search.maven.org/remotecontent?filepath=com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar

echo "-------------------------------------------------------------------------"
echo "Verify Apache Spark and Sedona versions"
echo "-------------------------------------------------------------------------"

${SPARK_HOME_DIR}/bin/spark-submit --version

echo "---------------------------------------"
pip list | grep "spark"
echo "---------------------------------------"
pip list | grep "sedona"
echo "---------------------------------------"
echo ""

echo "-------------------------------------------------------------------------"
echo "Run test Sedona script to prove everything is working"
echo "-------------------------------------------------------------------------"

python ${SCRIPT_DIR}/02_run_spatial_query.py

echo "----------------------------------------------------------------------------------------------------------------"

cd ${SCRIPT_DIR}

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
