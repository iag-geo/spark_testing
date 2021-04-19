#!/usr/bin/env bash

spark-submit --master yarn do_something.py

# connect to a Postgres RDS instance using SSH over SSM port forwarding
ssh -fNL 5433:my_rds_domain_name.ap-southeast-2.rds.amazonaws.com:5432 ${JUMPBOX_INSTANCE_ID}

# copy downloaded maven files to S3 for faster build process
aws s3 sync ~/.m2/repository s3://maven-downloads/sedona-1.3.2/repository




cd /Users/s57405/spark-3.0.1-with-sedona/jars || exit

# get hadoop-aws JAR file
mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ \
                   -Dartifact=org.apache.hadoop:hadoop-aws:3.2.0:jar \
                   -Dtransitive=false -Ddest=hadoop-aws-3.2.0.jar

# get aws-java-sdk JAR file
mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ \
                   -Dartifact=com.amazonaws:aws-java-sdk-bundle:1.11.880:jar \
                   -Dtransitive=false -Ddest=aws-java-sdk-bundle-1.11.880.jar

# get hadoop-aws JAR file
wget https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar

# get aws-java-sdk JAR file
wget https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk/1.11.880/aws-java-sdk-1.11.880.jar

# get Google Storage connector shaded JAR
wget https://search.maven.org/remotecontent?filepath=com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar
