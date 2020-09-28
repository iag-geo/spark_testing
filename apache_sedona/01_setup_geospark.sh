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
#   - Removes existing Spark install in $HOME/spark-$SPARK_VERSION-bin-hadoop2.7 folder
#   - Removes existing 'geospark_env' Conda environment
#
# PRE_REQUISITES:
#   1. Java SE SDK is installed and linked to /Library/Java/Home
#        - Install Java 8 using Homebrew: brew cask install adoptopenjdk8; or
#        - Install Java 11: brew cask install adoptopenjdk11
#   2. Miniconda installed in default directory ($HOME/opt/miniconda3)
#        - Get the installer here: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg
#
# ISSUES:
#   1. Conda environment variables aren't accessible in IntelliJ/Pycharm due to a missing feature
#        - Geospark python scripts will fail in IntelliJ/Pycharm as Spark env vars aren't set (e.g. $SPARK_HOME)
#
# --------------------------------------------------------------------------------------------------------------------
#
# SETUP:
#   - edit these if you know what versions go with what version!?
#       - for the current version of Geospark (1.3.1) and Conda: only Python 3.7 and Spark 2.4.6 are a valid combination

PYTHON_VERSION="3.7"
SPARK_VERSION="2.4.6"

# --------------------------------------------------------------------------------------------------------------------

# get directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SPARK_HOME_DIR="${HOME}/spark-${SPARK_VERSION}-bin-hadoop2.7"

# WARNING - remove existing spark install
rm -r ${SPARK_HOME_DIR}

echo "-------------------------------------------------------------------------"
echo "Downloading and Installing Apache Spark"
echo "-------------------------------------------------------------------------"

cd ${HOME}

# download and untar Spark files
wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
tar -xzf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

# add Postgres JDBC driver to Spark (optional - included for running xx_prep_abs_boundaries.py)
cd ${SPARK_HOME_DIR}/jars
wget https://jdbc.postgresql.org/download/postgresql-42.2.16.jar

echo "-------------------------------------------------------------------------"
echo "Creating new Conda Environment 'geospark_env'"
echo "-------------------------------------------------------------------------"

# stop the Conda environment currently running
conda deactivate

# WARNING - remove existing environment
conda env remove --name geospark_env

# update Conda platform
echo "y" | conda update conda

# Create Conda environment
echo "y" | conda create -n geospark_env python=${PYTHON_VERSION}

# activate and setup env
conda activate geospark_env
conda config --env --add channels conda-forge
conda config --env --set channel_priority strict

# add environment variables
conda env config vars set JAVA_HOME="/Library/Java/Home"
conda env config vars set SPARK_HOME="${SPARK_HOME_DIR}"
conda env config vars set SPARK_LOCAL_IP="127.0.0.1"
conda env config vars set PYSPARK_PYTHON="${HOME}/opt/miniconda3/envs/geospark_env/bin/python"
conda env config vars set PYSPARK_DRIVER_PYTHON="${HOME}/opt/miniconda3/envs/geospark_env/bin/python"
conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"

# reactivate for env vars to take effect
conda activate geospark_env

# install packages for geospark
echo "y" | conda install -c conda-forge pyspark=${SPARK_VERSION} jupyter
pip install geospark

echo "-------------------------------------------------------------------------"
echo "Verify Geospark version"
echo "-------------------------------------------------------------------------"

# confirm version of Geospark installed
conda list geospark

echo "-------------------------------------------------------------------------"
echo "Run test Geospark script to prove everything is working"
echo "-------------------------------------------------------------------------"

python ${SCRIPT_DIR}/02_run_spatial_query.py

echo "----------------------------------------------------------------------------------------------------------------"

cd ${HOME}

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
