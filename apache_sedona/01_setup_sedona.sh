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
#   1. Java 8 OpenJDK is installed
#        - Install using Homebrew:
#            brew install openjdk@8
#        - Edit .bash_profile:
#            export PATH="/usr/local/opt/openjdk@8/bin:$PATH"
#            export JAVA_HOME="/usr/local/opt/openjdk@8"
#        - Reload .bash_profile:
#            source .bash_profile
#
#   2. Miniconda installed in default directory ($HOME/opt/miniconda3)
#        - Get the installer here: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg
#
# ISSUES:
#   1. Conda environment variables aren't accessible in IntelliJ/Pycharm due to a missing feature
#        - Sedona python scripts will fail in IntelliJ/Pycharm as Spark env vars aren't set (e.g. $SPARK_HOME)
#
# --------------------------------------------------------------------------------------------------------------------
#
# SETUP:
#   - edit these if its now the future and versions have changed
#

PYTHON_VERSION="3.9"
SPARK_VERSION="3.0.1"
SEDONA_VERSION="1.0.0"

#SEDONA_BUILD_DIR="${HOME}/apache-sedona-${SEDONA_VERSION}-incubating-src"

# --------------------------------------------------------------------------------------------------------------------

# set the directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SPARK_HOME_DIR="${HOME}/spark-${SPARK_VERSION}-with-sedona"

# WARNING - remove existing spark install
rm -r ${SPARK_HOME_DIR}

echo "-------------------------------------------------------------------------"
echo "Downloading and Installing Apache Spark"
echo "-------------------------------------------------------------------------"

mkdir ${SPARK_HOME_DIR}
cd ${SPARK_HOME_DIR} || exit

# download and untar Spark files
wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz
tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz --directory ${SPARK_HOME_DIR} --strip-components=1
rm spark-${SPARK_VERSION}-bin-hadoop3.2.tgz

# create log4j properties file (based on Spark template)
cp ${SPARK_HOME_DIR}/conf/log4j.properties.template ${SPARK_HOME_DIR}/conf/log4j.properties

echo "-------------------------------------------------------------------------"
echo "Downloading additional JAR files"
echo "-------------------------------------------------------------------------"

cd ${SPARK_HOME_DIR}/jars || exit

# add Postgres JDBC driver to Spark (optional - included for running xx_prep_abs_boundaries.py)
wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar

# get hadoop-aws JAR file (optional - required for accessing AWS S3)
#wget https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar

# get aws-java-sdk JAR file (optional - required for accessing AWS S3)
#wget https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk/1.11.880/aws-java-sdk-1.11.880.jar

# get Google Storage connector shaded JAR (optional - required for accessing GCP Storage)
#wget https://search.maven.org/remotecontent?filepath=com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar

# create folder for Spark temp files
mkdir -p ${HOME}/tmp/spark

cd ${HOME} || exit

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
conda env config vars set JAVA_HOME="/usr/local/opt/openjdk@8"
conda env config vars set SPARK_HOME="${SPARK_HOME_DIR}"
conda env config vars set SPARK_LOCAL_IP="127.0.0.1"
conda env config vars set SPARK_LOCAL_DIRS="${HOME}/tmp/spark"
conda env config vars set PYSPARK_PYTHON="${HOME}/opt/miniconda3/envs/sedona/bin/python"
conda env config vars set PYSPARK_DRIVER_PYTHON="${HOME}/opt/miniconda3/envs/sedona/bin/ipython"
conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"

# reactivate for env vars to take effect
conda activate sedona

# install conda packages for Sedona
echo "y" | conda install -c conda-forge pyspark=${SPARK_VERSION} psycopg2 geopandas jupyter matplotlib

echo "-------------------------------------------------------------------------"
echo "Install Apache Sedona"
echo "-------------------------------------------------------------------------"

# step 1 - install from pip
pip install apache-sedona

# step 2 - add Sedona Python adapter JAR to Spark JAR files

## download source code
#mkdir ${SEDONA_BUILD_DIR}
#cd ${SEDONA_BUILD_DIR} || exit
#wget https://apache.mirror.digitalpacific.com.au/incubator/sedona/${SEDONA_VERSION}-incubating/apache-sedona-${SEDONA_VERSION}-incubating-src.tar.gz
#tar -xzf apache-sedona-${SEDONA_VERSION}-incubating-src.tar.gz --directory ${SEDONA_BUILD_DIR} --strip-components=1
#rm apache-sedona-${SEDONA_VERSION}-incubating-src.tar.gz
#
## build JAR with GeoTools included (GeoTools not included in binaries due to licensing)
#mvn clean install -DskipTests -Dscala=2.12 -Dspark=3.0 -Dgeotools
#
## copy to Spark JARs folder
#cp python-adapter/target/sedona-python-adapter-3.0_2.12-${SEDONA_VERSION}-incubating.jar ${SPARK_HOME_DIR}/jars/

## download and untar official Sedona python adapter JAR - doesn't include GeoTools, causing issues with a number of functions
#wget https://apache.mirror.digitalpacific.com.au/incubator/sedona/${SEDONA_VERSION}-incubating/apache-sedona-${SEDONA_VERSION}-incubating-bin.tar.gz
#tar -zxvf apache-sedona-${SEDONA_VERSION}-incubating-bin.tar.gz apache-sedona-${SEDONA_VERSION}-incubating-bin/sedona-python-adapter-3.0_2.12-${SEDONA_VERSION}-incubating.jar
#rm apache-sedona-${SEDONA_VERSION}-incubating-bin.tar.gz
## copy to Spark JARs folder
#cp apache-sedona-${SEDONA_VERSION}-incubating-bin/sedona-python-adapter-3.0_2.12-${SEDONA_VERSION}-incubating.jar ${SPARK_HOME_DIR}/jars/
#rm -R apache-sedona-${SEDONA_VERSION}-incubating-bin

# download unofficial shaded Sedona python adapter JAR with GeoTools
cd ${SPARK_HOME_DIR}/jars || exit
wget https://s3-ap-southeast-2.amazonaws.com/minus34.com/opensource/sedona-python-adapter-3.0_2.12-${SEDONA_VERSION}-incubating.jar

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

cd ${SCRIPT_DIR} || exit

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
