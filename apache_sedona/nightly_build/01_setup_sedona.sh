#!/usr/bin/env bash

SECONDS=0*

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"

# --------------------------------------------------------------------------------------------------------------------
# Script installs Apache Sedona & Apache Spark locally on a Mac, self-contained in a Conda environment
#
# Takes ~25 mins with an empty Maven repo; ~15 mins on subsequent runs (due to Maven caching)
#
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
POSTGRES_JDBC_VERSION="42.3.3"

# --------------------------------------------------------------------------------------------------------------------

# get the directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# this assumes miniconda is installed in the default directory on linux/MacOS
SPARK_HOME_DIR="${HOME}/opt/miniconda3/envs/sedona_nightly/lib/python${PYTHON_VERSION}/site-packages/pyspark"
INSTALL_DIR="${HOME}/opt/miniconda3/envs/sedona_nightly"

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
conda env config vars set PYSPARK_PYTHON="${INSTALL_DIR}/bin/python${PYTHON_VERSION}"
conda env config vars set PYSPARK_DRIVER_PYTHON="${INSTALL_DIR}/bin/ipython3"
conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"

## add environment variables for Maven
#conda env config vars set MAVEN_HOME="${HOME}/apache-maven-${MAVEN_VERSION}"
#conda env config vars set PATH="${MAVEN_HOME}/bin:${PATH}"

# reactivate for env vars to take effect
conda activate sedona_nightly

# install supporting & useful packages
conda install -y -c conda-forge pyspark psycopg sqlalchemy geoalchemy2 geopandas pyarrow jupyter matplotlib

## OPTIONAL - AWS Packages
#conda install -y -c conda-forge boto3 s3fs

echo "-------------------------------------------------------------------------"
echo "Installing Maven"
echo "-------------------------------------------------------------------------"

cd ${INSTALL_DIR}

curl -O "https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
tar xzf apache-maven-${MAVEN_VERSION}-bin.tar.gz
rm apache-maven-${MAVEN_VERSION}-bin.tar.gz

echo "-------------------------------------------------------------------------"
echo "Downloading, Building and Installing Apache Spark"
echo "-------------------------------------------------------------------------"

# download GitHub repo
curl -L -o master.zip https://github.com/apache/incubator-sedona/zipball/master/
unzip master.zip
rm master.zip
cd apache-incubator-sedona-*

# build Sedona (8 mins)
../apache-maven-${MAVEN_VERSION}/bin/mvn clean install -DskipTests -Dgeotools

# copy the JAR to Spark
cp python-adapter/target/sedona-python-adapter-*.jar ${SPARK_HOME_DIR}/jars/

# install Python module
cd python
python setup.py install

# create folder for Spark temp files
mkdir -p ${HOME}/tmp/spark

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

## copy Greenplum JDBC driver (must be downloaded manually after logging into VMWare site)
#cp ${HOME}/Downloads/greenplum-connector-apache-spark-scala_2.12-2.1.0/greenplum-connector-apache-spark-scala_2.12-2.1.0.jar .

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

python ${SCRIPT_DIR}/../02_run_spatial_query.py

echo "----------------------------------------------------------------------------------------------------------------"

cd ${SCRIPT_DIR}

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
