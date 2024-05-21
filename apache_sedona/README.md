# Apache Sedona Quick Start
Scripts for running geospatial analysis using Apache Sedona on Pyspark locally in a Jupyter notebook.

Tested on **macOS Intel & Apple Silicon**. Should work on Linux with some minor tweaking.

## Getting Started

Open Terminal and run *01_setup_sedona.sh* to create a self-contained Python 3.12 Conda environment with PySpark 3.5.x and Apache Sedona installed.

The script will finish by running *02_run_spatial_query.py* to confirm Apache Spark & Sedona is working. The test script takes <1 min to run.

## Pre-requisites

1. Java 11 OpenJDK is installed
     - Install using Homebrew: brew install openjdk@11

2. JAVA_HOME is set as an environment variable: export JAVA_HOME=/opt/homebrew/opt/openjdk@11 (if installed via brew)

3. Miniconda is installed in the default directory ($HOME/opt/miniconda3). Get the installer here:
     -  Intel: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg
     -  Apple Silicon: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.pkg 

## Running the Notebook

Assuming you still have Terminal open & the Conda *sedona* environment active:

1. Change the directory to this folder (if you're in another folder) 
2. Enter ```jupyter notebook``` to start the Jupyter Server.
3. Click on *02_run_spatial_query.ipynb* to open it.
