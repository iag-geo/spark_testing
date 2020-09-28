# Apache Sedona Quick Start
Scripts for running geospatial analysis using Apache Sedona (formerly Geospark) on Pyspark locally in a Jupyter notebook.

Scripts are currently for **macOS only**. Will work on Linux with some tweaking.

## Getting Started

Open terminal and run *01_setup_geospark.sh* to download & install Spark 2.4.x and create a self-contained Python 3.7 Conda environment with PySpark and Sedona (Geospark) installed.

The script will finish by running *02_run_spatial_query.py* to confirm Sedona is working.

## Pre-requisites

1. Java SE SDK is installed and linked to /Library/Java/Home
   - Install Java 8 using Homebrew: brew cask install adoptopenjdk8; or
   - Install Java 11: brew cask install adoptopenjdk11
2. Miniconda installed in default directory ($HOME/opt/miniconda3)
   - Get the installer here: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg

## Running the Notebook

Assuming you still have Terminal open & the Conda *geospark_env* environment active:

1. Enter ```jupyter notebook``` to start the Jupyter Server.
2. Click on *02_run_spatial_query.ipynb* to open it.
