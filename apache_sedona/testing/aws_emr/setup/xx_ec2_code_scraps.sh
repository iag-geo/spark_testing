#!/usr/bin/env bash

# copy downloaded maven files to S3 for faster build process
aws s3 sync ~/.m2/repository s3://maven-downloads/geospark-1.3.2/repository

