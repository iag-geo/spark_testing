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
#   - Removes existing Spark install in $HOME/spark-$SPARK_VERSION-with-sedona folder
#   - Removes existing 'sedona' Conda environment
#
# PRE_REQUISITES:
#   1. Java 8 OpenJDK is installed using Homebrew: brew install openjdk@11
#
#   2. Miniconda is installed in the default directory ($HOME/opt/miniconda3)
#        - Get the installer here: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg
#
# ISSUES:
#   1. Conda environment variables aren't accessible in IntelliJ/Pycharm due to a missing feature
#        - Sedona Python scripts will fail in IntelliJ/Pycharm as Spark env vars aren't set (e.g. $SPARK_HOME)
#
# --------------------------------------------------------------------------------------------------------------------
#
# SETUP:
#   - edit these if it's now the future and versions have changed
#   - note: check which Spark versions Sedona supports
#

PYTHON_VERSION="3.9"
SPARK_VERSION="3.1.2"
POSTGRES_JDBC_VERSION="42.2.22"

# --------------------------------------------------------------------------------------------------------------------

# get the directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SPARK_HOME_DIR="${HOME}/spark-${SPARK_VERSION}-with-sedona"

# WARNING - remove existing spark install
rm -r ${SPARK_HOME_DIR}

echo "-------------------------------------------------------------------------"
echo "Downloading and Installing Apache Spark"
echo "-------------------------------------------------------------------------"

mkdir ${SPARK_HOME_DIR}
cd ${SPARK_HOME_DIR}

# download and untar Spark files
curl -O https://apache.mirror.digitalpacific.com.au/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz
tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz --directory ${SPARK_HOME_DIR} --strip-components=1
rm spark-${SPARK_VERSION}-bin-hadoop3.2.tgz

# create log4j properties file (based on Spark template)
cp ${SPARK_HOME_DIR}/conf/log4j.properties.template ${SPARK_HOME_DIR}/conf/log4j.properties

echo "-------------------------------------------------------------------------"
echo "Downloading additional JAR files"
echo "-------------------------------------------------------------------------"

cd ${SPARK_HOME_DIR}/jars

# add Postgres JDBC driver to Spark (optional - included for running xx_prep_abs_boundaries.py)
curl -O https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

# get hadoop-aws & aws-java-sdk JAR files (optional - required for accessing AWS S3)
#curl -O https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
#curl -O https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk/1.11.880/aws-java-sdk-1.11.880.jar

# get Google Storage connector shaded JAR (optional - required for accessing GCP Storage)
#curl -O https://search.maven.org/remotecontent?filepath=com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar

# create folder for Spark temp files
mkdir -p ${HOME}/tmp/spark

cd ${HOME}

echo "-------------------------------------------------------------------------"
echo "Creating new Conda Environment 'sedona'"
echo "-------------------------------------------------------------------------"

# stop the Conda environment currently running
conda deactivate

# WARNING - remove existing environment
conda env remove --name sedona

# update Conda platform
echo "y" | conda update conda

# Create Conda environment
echo "y" | conda create -n sedona python=${PYTHON_VERSION}

# activate and setup env
conda activate sedona
conda config --env --add channels conda-forge
conda config --env --set channel_priority strict

# add environment variables for Pyspark
conda env config vars set JAVA_HOME="/usr/local/opt/openjdk@11"
conda env config vars set SPARK_HOME="${SPARK_HOME_DIR}"
conda env config vars set SPARK_LOCAL_IP="127.0.0.1"
conda env config vars set SPARK_LOCAL_DIRS="${HOME}/tmp/spark"
conda env config vars set PYSPARK_PYTHON="${HOME}/opt/miniconda3/envs/sedona/bin/python"
conda env config vars set PYSPARK_DRIVER_PYTHON="${HOME}/opt/miniconda3/envs/sedona/bin/ipython"
conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"

# reactivate for env vars to take effect
conda activate sedona

# install supporting & useful packages
echo "y" | conda install -c conda-forge psycopg2 sqlalchemy geoalchemy2 geopandas pyarrow jupyter matplotlib

## OPTIONAL - AWS Packages
#echo "y" | conda install -c conda-forge boto3

echo "-------------------------------------------------------------------------"
echo "Install Apache Sedona"
echo "-------------------------------------------------------------------------"

pip install apache-sedona

# fix Sedona 1.0.1 packaging issue (uninstall Pyspark 3.0.2 and install required version)
echo "y" | pip uninstall pyspark
echo "y" | conda install -c conda-forge pyspark=${SPARK_VERSION}

echo "-------------------------------------------------------------------------"
echo "Verify Sedona version"
echo "-------------------------------------------------------------------------"

# confirm version of Sedona installed
pip list | grep "sedona"

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
