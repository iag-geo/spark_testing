# Apache Sedona Quick Start
Scripts for setting up PySpark and running geospatial analysis on it using Apache Sedona (formerly Geospark) in a Jupyter notebook.

Scripts are currently for **macOS only**. Will work on Linux with some tweaking.

## Getting Started

Run *01_setup_geospark.sh* to download & install Spark 2.4.x and create a self-contained Python 3.7 Conda environment with PySpark and Sedona (Geospark) installed.

The setup script will finish by running a test geospatial analysis script to confirm Sedona is working.

## Pre-requisites

1. Java SE SDK is installed and linked to /Library/Java/Home
   - Install Java 8 using Homebrew: brew cask install java8; or
   - Install Java 11: brew cask install java11
2. Miniconda installed in default directory ($HOME/opt/miniconda3)
   - Get the installer here: https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.pkg

## Running the Notebook

1. Open Terminal
2. Go to the folder you downloaded this repo to:

   ```cd /<your_download_folder>/spark_testing/apache_sedona```

3. Start the Jupyter server

   ```jupyter notebook```

4. Click on the *02_run_spatial_query* file
