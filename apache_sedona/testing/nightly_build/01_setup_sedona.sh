#!/usr/bin/env bash

SECONDS=0*

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"

# --------------------------------------------------------------------------------------------------------------------
# Script installs Apache Sedona & Apache Spark locally on a Mac, self-contained in a Conda environment
# --------------------------------------------------------------------------------------------------------------------
#
# Author: Hugh Saalmans, IAG Strategy & Innovation
# Date: 2020-09-25
#
# WARNINGS:
#   - Removes existing 'sedona_nightly' Conda environment
#
# PRE_REQUISITES:
#   1. Java 11 OpenJDK is installed using Homebrew: brew install openjdk@11
#
#   2. Miniconda is installed in the default directory ($HOME/opt/miniconda3)
#        - Mac Intel installer: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg
#
#   3. Maven is installed, including the $MAVEN_HOME env variable set and the $MAVEN_HOME/bin folder added to $PATH
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

PYTHON_VERSION="3.10"
MAVEN_VERSION="3.8.5"
#SPARK_VERSION="3.2.1"  # uncomment to install specific version of Spark
#SEDONA_VERSION="1.2.0"
#SCALA_VERSION="2.12"
GEOTOOLS_VERSION="25.2"
POSTGRES_JDBC_VERSION="42.3.3"

# --------------------------------------------------------------------------------------------------------------------

# get the directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# this assumes miniconda is installed in the default directory on linux/MacOS
SPARK_HOME_DIR="${HOME}/opt/miniconda3/envs/sedona_nightly/lib/python${PYTHON_VERSION}/site-packages/pyspark"
SEDONA_INSTALL_DIR="${HOME}/opt/miniconda3/envs/sedona_nightly/apache-sedona"

# WARNING - remove existing Sedona source code directory
rm -rf ${SEDONA_INSTALL_DIR}

cd ${HOME}

echo "-------------------------------------------------------------------------"
echo "Creating new Conda Environment 'sedona_nightly'"
echo "-------------------------------------------------------------------------"

# stop the current Conda environment (if any running)
conda deactivate

# WARNING - remove existing environment
conda env remove --name sedona_nightly

# update Conda platform
conda update -y conda

# create Conda environment
conda create -y -n sedona_nightly python=${PYTHON_VERSION}

# activate env
conda activate sedona_nightly

# add environment variables for Pyspark
conda env config vars set JAVA_HOME="/usr/local/opt/openjdk@11"
conda env config vars set SPARK_HOME="${SPARK_HOME_DIR}"
conda env config vars set SPARK_LOCAL_IP="127.0.0.1"
conda env config vars set SPARK_LOCAL_DIRS="${HOME}/tmp/spark"
conda env config vars set PYSPARK_PYTHON="${HOME}/opt/miniconda3/envs/sedona_nightly/bin/python${PYTHON_VERSION}"
conda env config vars set PYSPARK_DRIVER_PYTHON="${HOME}/opt/miniconda3/envs/sedona_nightly/bin/ipython3"
conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"

# add environment variables for Maven
conda env config vars set MAVEN_HOME="${HOME}/apache-maven-${MAVEN_VERSION}"
conda env config vars set PATH="${MAVEN_HOME}/bin:${PATH}"

# reactivate for env vars to take effect
conda activate sedona_nightly

# install supporting & useful packages
conda install -y -c conda-forge pyspark psycopg2 sqlalchemy geoalchemy2 geopandas pyarrow jupyter matplotlib

## OPTIONAL - AWS Packages
#conda install -y -c conda-forge boto3 s3fs

#echo "-------------------------------------------------------------------------"
#echo "Install Maven"
#echo "-------------------------------------------------------------------------"
#
#cd ${HOME}
#
#curl -O https://www.strategylions.com.au/mirror/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz
#tar xzf apache-maven-$MAVEN_VERSION-bin.tar.gz
#rm apache-maven-$MAVEN_VERSION-bin.tar.gz

echo "-------------------------------------------------------------------------"
echo "Download, Build and Install Apache Spark"
echo "-------------------------------------------------------------------------"

mkdir ${SEDONA_INSTALL_DIR}
cd ${SEDONA_INSTALL_DIR}

# download GitHub repo
curl -L -o master.zip https://github.com/apache/incubator-sedona/zipball/master/
unzip master.zip
rm master.zip
cd apache-incubator-sedona-*

# build Sedona (8 mins)
mvn clean install -DskipTests -Dgeotools









#echo "-------------------------------------------------------------------------"
#echo "Downloading additional JAR files"
#echo "-------------------------------------------------------------------------"
#
#cd ${SPARK_HOME_DIR}/jars
#
## add Postgres JDBC driver to Spark (optional - included for running xx_prep_abs_boundaries.py)
#curl -O https://jdbc.postgresql.org/download/postgresql-42.2.19.jar
#
## get hadoop-aws JAR file (optional - required for accessing AWS S3)
##curl -O https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
#
## get aws-java-sdk JAR file (optional - required for accessing AWS S3)
##curl -O https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk/1.11.880/aws-java-sdk-1.11.880.jar
#
## get Google Storage connector shaded JAR (optional - required for accessing GCP Storage)
##curl -O https://search.maven.org/remotecontent?filepath=com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar
#
## create folder for Spark temp files
#mkdir -p ${HOME}/tmp/spark
#
#cd ${HOME}
#
#echo "-------------------------------------------------------------------------"
#echo "Creating new Conda Environment 'sedona-nightly'"
#echo "-------------------------------------------------------------------------"
#
## stop the Conda environment currently running
#conda deactivate
#
## WARNING - remove existing environment
#conda env remove --name sedona-nightly
#
## update Conda platform
#echo "y" | conda update conda
#
## Create Conda environment
#echo "y" | conda create -n sedona-nightly python=${PYTHON_VERSION}
#
## activate and setup env
#conda activate sedona-nightly
#conda config --env --add channels conda-forge
#conda config --env --set channel_priority strict
#
## add environment variables for Pyspark
#conda env config vars set JAVA_HOME="/usr/local/opt/openjdk@8"
##conda env config vars set MAVEN_HOME="${HOME}/apache-maven-${MAVEN_VERSION}"
#conda env config vars set SPARK_HOME="${SPARK_HOME_DIR}"
#conda env config vars set SPARK_LOCAL_IP="127.0.0.1"
#conda env config vars set SPARK_LOCAL_DIRS="${HOME}/tmp/spark"
#conda env config vars set PYSPARK_PYTHON="${HOME}/opt/miniconda3/envs/sedona-nightly/bin/python"
#conda env config vars set PYSPARK_DRIVER_PYTHON="${HOME}/opt/miniconda3/envs/sedona-nightly/bin/ipython"
#conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"
#
## reactivate for env vars to take effect
#conda activate sedona-nightly
#
## install conda packages for Sedona
#echo "y" | conda install -c conda-forge pyspark=${SPARK_VERSION} pyspark-stubs psycopg2 geopandas jupyter matplotlib boto3
#
#echo "-------------------------------------------------------------------------"
#echo "Build & Install Apache Sedona"
#echo "-------------------------------------------------------------------------"
#
## download it
#cd ${HOME}
#git clone https://github.com/apache/incubator-sedona.git
#
## Build it
#cd ${SEDONA_INSTALL_DIR}
#mvn clean install -DskipTests -Dgeotools
#
## install it
#
## Copy Sedona JARs to Spark install
#cp ${SEDONA_INSTALL_DIR}/python-adapter/target/sedona-python-adapter-3.0_2.12-${SEDONA_VERSION}.jar ${SPARK_HOME}/jars
#
## install Sedona in Python from local setup.py
#cd ${SEDONA_INSTALL_DIR}/python
#python setup.py install
#
#echo "-------------------------------------------------------------------------"
#echo "Verify Sedona version"
#echo "-------------------------------------------------------------------------"
#
## confirm version of Sedona installed
#pip list | grep "sedona"
#
#echo "-------------------------------------------------------------------------"
#echo "Run test Sedona script to prove everything is working"
#echo "-------------------------------------------------------------------------"
#
#python ${SCRIPT_DIR}/02_run_spatial_query.py
#
#echo "----------------------------------------------------------------------------------------------------------------"

cd ${SCRIPT_DIR}

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
