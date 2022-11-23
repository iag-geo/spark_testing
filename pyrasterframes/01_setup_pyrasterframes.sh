#!/usr/bin/env bash

SECONDS=0*

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"

# --------------------------------------------------------------------------------------------------------------------
# Script installs PyRasterFrames locally on a Mac with Spark in standalone mode
# --------------------------------------------------------------------------------------------------------------------
#
# Author: Hugh Saalmans, IAG Strategy & Innovation
# Date: 2020-11-09
#
# WARNINGS:
#   - Removes existing Spark install in $HOME/spark-$SPARK_VERSION-with-pyrasterframes folder
#   - Removes existing 'pyrasterframes_env' Conda environment
#
# PRE_REQUISITES:
#   1. Java 8 OpenJDK is installed
#        - Install using Homebrew: brew cask install adoptopenjdk8
#
#   2. Miniconda installed in default directory ($HOME/opt/miniconda3)
#        - Get the installer here: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg
#
# ISSUES:
#   1. Conda environment variables aren't accessible in IntelliJ/Pycharm due to a missing feature
#        - PyRasterFrames python scripts will fail in IntelliJ/Pycharm as Spark env vars aren't set (e.g. $SPARK_HOME)
#
# --------------------------------------------------------------------------------------------------------------------
#
# SETUP:
#   - edit these if you know what versions go with what version!?
#       - for the current version of PyRasterFrames (0.9.0) and Conda: only Python 3.7 & Spark 2.4.5 are a valid combo

PYTHON_VERSION="3.7"
GDAL_VERSION="2.4.4"
SPARK_VERSION="2.4.5"

# --------------------------------------------------------------------------------------------------------------------

# get directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SPARK_HOME_DIR="${HOME}/spark-${SPARK_VERSION}-with-pyrasterframes"

# WARNING - remove existing spark install
rm -r ${SPARK_HOME_DIR}

echo "-------------------------------------------------------------------------"
echo "Downloading and Installing Apache Spark"
echo "-------------------------------------------------------------------------"

mkdir ${SPARK_HOME_DIR}
cd ${SPARK_HOME_DIR}

# download and untar Spark files
curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
tar -xzf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz --directory ${SPARK_HOME_DIR} --strip-components=1
rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

# add Postgres JDBC driver to Spark (optional - included for running xx_prep_abs_boundaries.py)
cd ${SPARK_HOME_DIR}/jars
curl -O https://jdbc.postgresql.org/download/postgresql-42.2.18.jar

# create folder for Spark temp files
mkdir -p ${HOME}/tmp/spark

cd ${HOME}

echo "-------------------------------------------------------------------------"
echo "Creating new Conda Environment 'pyrasterframes_env'"
echo "-------------------------------------------------------------------------"

# stop the Conda environment currently running
conda deactivate

# WARNING - remove existing environment
conda env remove --name pyrasterframes_env

# update Conda platform
echo "y" | conda update conda

# Create Conda environment
echo "y" | conda create -n pyrasterframes_env python=${PYTHON_VERSION}

# activate and setup env
conda activate pyrasterframes_env
conda config --env --add channels conda-forge
conda config --env --set channel_priority strict

# add environment variables
conda env config vars set JAVA_HOME="/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
conda env config vars set SPARK_HOME="${SPARK_HOME_DIR}"
conda env config vars set SPARK_LOCAL_IP="127.0.0.1"
conda env config vars set SPARK_LOCAL_DIRS="${HOME}/tmp/spark"
conda env config vars set PYSPARK_PYTHON="${HOME}/opt/miniconda3/envs/pyrasterframes_env/bin/python"
conda env config vars set PYSPARK_DRIVER_PYTHON="${HOME}/opt/miniconda3/envs/pyrasterframes_env/bin/ipython"
conda env config vars set PYLIB="${SPARK_HOME_DIR}/python/lib"

# reactivate for env vars to take effect
conda activate pyrasterframes_env

# install packages for PyRasterFrames
echo "y" | conda install -c conda-forge pyspark=${SPARK_VERSION} gdal=${GDAL_VERSION} psycopg jupyter matplotlib
pip install pyrasterframes

echo "-------------------------------------------------------------------------"
echo "Verify PyRasterFrames version"
echo "-------------------------------------------------------------------------"

# confirm version of PyRasterFrames installed
conda list pyrasterframes

#echo "-------------------------------------------------------------------------"
#echo "Run test PyRasterFrames script to prove everything is working"
#echo "-------------------------------------------------------------------------"
#
#python ${SCRIPT_DIR}/02_run_spatial_query.py

echo "----------------------------------------------------------------------------------------------------------------"

cd ${HOME}

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
