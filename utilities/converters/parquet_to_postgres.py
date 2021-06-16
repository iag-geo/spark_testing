# --------------------------------------------------------------------------------------------------------------------
#
# Download S3 parquet files or open local parquet files and export to postgres
#
# Supports complex/nested data types and Well Known Text (WKT) geometries
#
# --------------------------------------------------------------------------------------------------------------------
#
# Author: Hugh Saalmans, IAG Innovation & Ventures
# Date: 2021-06-16
#
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# WARNING:
#   - WILL REPLACE THE TARGET POSTGRES TABLE if it already exists
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#
# PRE_REQUISITES:
#   - Install these Python packages: pyarrow, geopandas, numpy, boto3, psycopg2, geoalchemy2
#   - (optional) if exporting S3 files - setup your AWS credentials:
#      - https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
#
# --------------------------------------------------------------------------------------------------------------------
#
# PARAMETERS:
#     --aws-profile : string
#         The AWS profile with access to your S3 bucket
#     --source-s3-bucket : string
#         The S3 bucket for the parquet file(s)
#     --source-s3-folder : string
#         The S3 folder for the parquet file(s)
#     --source-local-folder : string
#         The local folder of the parquet file(s)
#     --json-fields : space delimited string(s)
#         List of complex/nested type columns that will be converted to jsonb in postgres
#     --geom-field : string
#         The WKT or EWKT geometry column to convert to PostGIS geometries
#     --geom-type : string
#         The WKT geometry type - https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
#     --srid : integer
#         What's the coordinate system EPSG number (if spatial)
#     --target-table : string
#         The schema and table name (e.g. <schemaname>.<tablename>) for the target Postgres table
#
# --------------------------------------------------------------------------------------------------------------------
#
# NOTES:
#   - Complex/nested types (aka structs) are supported, these are converted to JSONB in Postgres
#   - Well Known Text (WKT) and EWKT geometries are converted to PostGIS geometries and spatially indexed
#   - If you have mixed geometries (e.g. points and lines) - choose the GEOMETRY or GEOMETRYM type
#   - Not tested with custom binary objects - you'll most likely have to
#   - Files will be exported in parallel. Half your CPUs is the default number of processes
#
# --------------------------------------------------------------------------------------------------------------------
#
# SETUP:
#   - edit the postgres connect string
#   - (optional) set the number of CPUs to use
#
# ---------------------------------------------------------------------------------------------------------------------

import argparse
import boto3
import geopandas
import glob
import json
import multiprocessing
import math
import numpy
import os
import pandas
import shutil
import sqlalchemy
import sys

from datetime import datetime
from pathlib import Path
from sqlalchemy.dialects.postgresql import JSONB

# -- START EDIT SETTINGS ----------------------------------------------------------------------------------------------

# postgres connect string - format: "postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>"
sql_alchemy_engine_string = "postgresql+psycopg2://postgres:password@localhost:5432/geo"

# number of parallel process to use (default is half your CPUs)
cpu_count = math.floor(multiprocessing.cpu_count() / 2)

# -- END EDIT SETTINGS ------------------------------------------------------------------------------------------------

# local temp folder for downloading parquet files
temp_folder = "/Users/s57405/tmp/aws_s3/tmp"


def main():
    start_time = datetime.now()

    # create AWS s3 client and get global variables
    settings = initialize()

    # set correct AWS user
    boto3.setup_default_session(profile_name=settings["aws_profile"])

    job_list = list()
    i = 0

    if settings["s3_folder"] is not None:
        # setup for S3 file list
        print("\t- importing {}".format(settings["s3_folder"]))

        # create directory path if missing
        Path(settings["input_folder"]).mkdir(parents=True, exist_ok=True)

        # delete local temporary files (if they exist)
        delete_files_in_folder(settings["input_folder"])

        # get list of S3 files to process
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(settings["s3_bucket"])
        objs = bucket.objects.filter(Prefix=settings["s3_folder"])

        for obj in objs:
            key = str(obj.key)

            # filter out non-parquet files
            if key.endswith(".parquet"):
                job_list.append((settings, i, key, os.path.join(temp_folder, key)))
                i += 1

    else:
        # setup for local file list
        print("\t- importing {}".format(settings["input_folder"]))

        file_list = glob.glob(os.path.join(settings["input_folder"], "*.parquet"))

        for file_name in file_list:
            job_list.append((settings, i, None, file_name))
            i += 1

    print("\t- {} files to load: {}".format(len(job_list), datetime.now() - start_time))
    start_time = datetime.now()

    # download, process and export the first file to new Postgres table - this will create the table structure
    download_and_import(job_list[0])

    # remove first file from job list
    job_list.pop(0)

    print("\t\t\t- created table based on first file's schema: {}".format(datetime.now() - start_time))
    start_time = datetime.now()

    # make a process pool and download, process and append all remaining files in parallel
    pool = multiprocessing.Pool(cpu_count, initialize)
    results = pool.imap_unordered(download_and_import, job_list)
    pool.close()
    pool.join()

    # check parallel processing results
    for result in results:
        if result is not None:
            print("WARNING: multiprocessing error : {}".format(result))

    print("\t- processed {} files : {}".format(i, datetime.now() - start_time))
    start_time = datetime.now()

    # add indexed geom column to Postgres table (if required) and cluster & analyse table
    sql_engine = sqlalchemy.create_engine(sql_alchemy_engine_string, isolation_level="AUTOCOMMIT")
    with sql_engine.connect() as conn:
        if settings["geom_field"] is not None:
            # conn.execute("ALTER TABLE {}.{} ADD COLUMN geom geometry({}, {})"
            #              .format(settings["schema_name"], settings["table_name"],
            #                      settings["geom_type"], settings["srid"]))
            # conn.execute("UPDATE {}.{} SET geom = st_geomfromewkt({})"
            #              .format(settings["schema_name"], settings["table_name"], settings["geom_field"]))
            # conn.execute("ALTER TABLE {}.{} DROP COLUMN {}"
            #              .format(settings["schema_name"], settings["table_name"], settings["geom_field"]))
            # conn.execute("CREATE INDEX idx_{1}_geom ON {0}.{1} USING gist (geom)"
            #              .format(settings["schema_name"], settings["table_name"]))
            # conn.execute("ALTER TABLE {0}.{1} CLUSTER ON idx_{1}_geom"
            #              .format(settings["schema_name"], settings["table_name"]))
            # conn.execute("VACUUM FULL {}.{}".format(settings["schema_name"], settings["table_name"]))
            conn.execute("ALTER TABLE {0}.{1} CLUSTER ON idx_{1}_geometry"
                         .format(settings["schema_name"], settings["table_name"]))
            conn.execute("ALTER TABLE {}.{} RENAME COLUMN geometry TO geom"
                         .format(settings["schema_name"], settings["table_name"]))

        conn.execute("ANALYSE {}.{}".format(settings["schema_name"], settings["table_name"]))

    print("\t- geometries (optionally) added & table optimised : {}".format(datetime.now() - start_time))
    start_time = datetime.now()

    # delete temporary files if source was S3
    if settings["s3_folder"] is not None:
        delete_files_in_folder(settings["input_folder"])
        print("Files deleted : {}".format(datetime.now() - start_time))


# get user parameters - outputs a dictionary of parameters
def initialize():
    parser = argparse.ArgumentParser(
        description="downloads S3 parquet files or loads local files, and imports them into postgres")

    parser.add_argument("--aws-profile",
                        default="default", help="The AWS profile with access to your S3 bucket")
    parser.add_argument("--source-s3-bucket",
                        help="The S3 bucket for the CSV file(s)")
    parser.add_argument("--source-s3-folder",
                        help="The S3 folder for the parquet file(s)")
    parser.add_argument("--source-local-folder",
                        help="The local folder for the parquet file(s)")
    parser.add_argument('--json-fields', nargs='+', default=[],
                        help='List of complex object columns that will be converted to jsonb in postgres')
    parser.add_argument("--geom-field",
                        help="WKT or EWKT geometry column to convert to PostGIS geometries")
    parser.add_argument("--geom-type", choices=["GEOMETRYCOLLECTION", "GEOMETRY", "POINT", "MULTIPOINT",
                                                    "LINESTRING", "MULTILINESTRING", "POLYGON", "MULTIPOLYGON",
                                                    "GEOMETRYM", "POINTM", "MULTIPOINTM", "LINESTRINGM",
                                                    "MULTILINESTRINGM", "POLYGONM", "MULTIPOLYGONM",
                                                    "TIN", "POLYHEDRALSURFACE"],
                        type=str.upper, default="GEOMETRY", help="The WKT geometry type - https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry")
    parser.add_argument("--srid",
                        default="4326", help="The coordinate system EPSG number (if spatial)")
    parser.add_argument("--target-table", required=True,
                        help="The schema and table name (e.g. <schemaname>.<tablename>) for the target Postgres table")

    args = parser.parse_args()

    settings = dict()

    settings["aws_profile"] = args.aws_profile
    settings["s3_bucket"] = args.source_s3_bucket
    settings["s3_folder"] = args.source_s3_folder
    settings["local_folder"] = args.source_local_folder
    settings["json_fields"] = args.json_fields
    settings["geom_field"] = args.geom_field
    settings["geom_type"] = args.geom_type
    settings["schema_name"] = args.target_table.split(".")[0]
    settings["table_name"] = args.target_table.split(".")[1]
    settings["srid"] = args.srid

    if settings["s3_folder"] is not None:
        # remove leading slash if present - stuffs up path join
        if settings["s3_folder"][0] == "/":
            settings["s3_folder"] = settings["s3_folder"][1:]

        settings["input_folder"] = os.path.join(temp_folder, settings["s3_folder"])

    elif settings["local_folder"] is not None:
        settings["input_folder"] = settings["local_folder"]

    else:
        print("FATAL: MISSING INPUT S3 OR LOCAL FILES FOLDER - EXITING")
        exit()

    return settings


# download a file (if in S3), open file as a Pandas dataframe, using Pyarrow, and export to Postgres
def download_and_import(job):
    start_time = datetime.now()

    settings, file_num, key, file_name = job

    # get file from AWS S3
    if settings["s3_folder"] is not None:
        # get an S3 client
        s3_client = boto3.client("s3")
        s3_client.download_file(settings["s3_bucket"], key, file_name)

    # if the first file being processed - replace existing Postgres table, otherwise append
    if file_num == 0:
        table_mode = "replace"
    else:
        table_mode = "append"

    # import parquet file into a Pandas dataframe
    # df = pandas.read_parquet(file_name, engine="pyarrow", read_dictionary=settings["json_fields"])
    df = pandas.read_parquet(file_name, engine="pyarrow")

    force_json_dict = dict()

    # convert list and dict (i.e Parquet complex types) columns to JSON columns
    for field_name in settings["json_fields"]:
        df[field_name] = list(map(lambda x: json.dumps(x, cls=NumpyArrayEncoder), df[field_name]))

        # create dict of these columns to force as json in Postgres
        force_json_dict[field_name] = JSONB

    # add SRID to WKT geometry column if needed
    if settings["geom_field"] is not None:
        output_df = geopandas.GeoDataFrame(df,
                                           geometry=geopandas.GeoSeries.from_wkt(df[settings["geom_field"]].str.replace("SRID={};".format(settings["srid"]), "")),
                                           crs="EPSG:{}".format(settings["srid"])) \
            .drop(settings["geom_field"], axis=1)
    else:
        output_df = df

        # srid_string = "SRID={};".format(settings["srid"])
        # test_geom = df[settings["geom_field"]][0]
        # # print(test_geom)
        #
        # # test if field is WKT or EWKT
        # if len(test_geom.split(";")) == 1:
        #     # WKT - add SRID to make EWKT geoms
        #     df[settings["geom_field"]] = srid_string + df[settings["geom_field"]]

    # create database engine
    sql_engine = sqlalchemy.create_engine(sql_alchemy_engine_string, isolation_level="AUTOCOMMIT")

    # Export to Postgres
    output_df.to_postgis(settings["table_name"], sql_engine, schema=settings["schema_name"],
                         if_exists=table_mode, index=False, dtype=force_json_dict)

    print("\t\t- imported {} into Postgres : {}"
          .format(os.path.basename(file_name), datetime.now() - start_time))


def delete_files_in_folder(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("Failed to delete %s. Reason: %s" % (file_path, e))


class NumpyArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


if __name__ == "__main__":
    full_start_time = datetime.now()

    task_name = "Parquet to Postgres Import"

    print("{} started".format(task_name))
    print("\t- running on Python {}".format(sys.version.replace("\n", " ")))

    main()

    time_taken = datetime.now() - full_start_time
    print("{} finished : {}".format(task_name, time_taken))
    print()
