#!/usr/bin/env bash

spark-submit --master yarn do_something.py

# connect to a Postgres RDS instance using SSH over SSM port forwarding
ssh -fNL 5433:my_rds_domain_name.ap-southeast-2.rds.amazonaws.com:5432 ${JUMPBOX_INSTANCE_ID}

# copy downloaded maven files to S3 for faster build process
aws s3 sync ~/.m2/repository s3://maven-downloads/geospark-1.3.2/repository


# get hadoop-aws JAR direct URL
mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ \
                   -DgroupId=org.apache.hadoop -DartifactId=hadoop-aws -Dversion=3.2.1 \
                   -Dtransitive=false

mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ \
                   -DgroupId=com.amazonaws -DartifactId=aws-java-sdk-bundle -Dversion=1.11.375 \
                   -Dtransitive=false


