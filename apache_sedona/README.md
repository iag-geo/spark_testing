# Apache Sedona Quick Start
Scripts for running geospatial analysis using Apache Sedona (formerly Geospark) on Pyspark locally in a Jupyter notebook.

Tested on **macOS only**. Should work on Linux with some minor tweaking.

## Getting Started

Open Terminal and run *01_setup_sedona.sh* to download & install Spark 3.0.x and create a self-contained Python 3.9 Conda environment with PySpark and Apache Sedona installed.

The script will finish by running *02_run_spatial_query.py* to confirm Sedona is working. The test script takes ~1 min to run.

## Pre-requisites

1. Java 8 OpenJDK is installed
     - Install using Homebrew: ```brew install openjdk@8```
     - Add the following lines to your .bash_profile file:
         - *export PATH="/usr/local/opt/openjdk@8/bin:$PATH"*
         - *export JAVA_HOME="/usr/local/opt/openjdk@8"*
     - Reload .bash_profile: ```source .bash_profile```

2. Miniconda installed in default directory ($HOME/opt/miniconda3)
     - Get the installer here: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg

## Running the Notebook

Assuming you still have Terminal open & the Conda *sedona* environment active:

1. Change the directory to this folder (if you're in another folder) 
2. Enter ```jupyter notebook``` to start the Jupyter Server.
3. Click on *02_run_spatial_query.ipynb* to open it.