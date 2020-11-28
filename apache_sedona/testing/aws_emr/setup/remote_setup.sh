#!/usr/bin/env bash

SECONDS=0*

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"

echo "-------------------------------------------------------------------------"
echo " Set temp local environment vars"
echo "-------------------------------------------------------------------------"

POSTGRES_JDBC_VERSION="42.2.18"
MAVEN_VERSION=3.6.3

GEOSPARK_INSTALL_DIR=~/incubator-sedona-1.3.2-spark-3.0

export PATH=/usr/lib/apache-maven-$MAVEN_VERSION/bin:$PATH

export JAVA_HOME="/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre"
export SPARK_HOME="/usr/lib/spark"
export HADOOP_HOME="/usr/lib/hadoop"

echo "-------------------------------------------------------------------------"
echo "Installing Maven"
echo "-------------------------------------------------------------------------"

wget -q -e use_proxy=yes --no-check-certificate \
https://www.strategylions.com.au/mirror/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz
tar xzf apache-maven-$MAVEN_VERSION-bin.tar.gz
sudo mv apache-maven-$MAVEN_VERSION /usr/lib/apache-maven-$MAVEN_VERSION
rm apache-maven-$MAVEN_VERSION-bin.tar.gz

echo "-------------------------------------------------------------------------"
echo " Copy files to EMR instance"
echo "-------------------------------------------------------------------------"

# download Postgres JDBC JAR
wget -q -e use_proxy=yes --no-check-certificate \
https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar
sudo mv postgresql-${POSTGRES_JDBC_VERSION}.jar ${SPARK_HOME}/jars/

echo "-------------------------------------------------------------------------"
echo " Build & install Apache Sedona"
echo "-------------------------------------------------------------------------"

# download Apache Sedona source code
wget -q -e use_proxy=yes --no-check-certificate \
https://github.com/apache/incubator-sedona/archive/1.3.2-spark-3.0.tar.gz
tar xzf 1.3.2-spark-3.0.tar.gz
rm 1.3.2-spark-3.0.tar.gz

# copy maven files from S3 for faster build process
mkdir ~/.m2/repository
aws s3 sync s3://maven-downloads/geospark-1.3.2/repository ~/.m2/repository

# Build it
cd ${GEOSPARK_INSTALL_DIR} || exit
mvn clean install -DskipTests \
-Dmaven.wagon.http.ssl.insecure=true \
-Dmaven.wagon.http.ssl.allowall=true \
-Dmaven.wagon.http.ssl.ignore.validity.dates=true

# Copy JARs to Spark folder
sudo cp ${GEOSPARK_INSTALL_DIR}/core/target/geospark-1.3.2-SNAPSHOT.jar ${SPARK_HOME}/jars/
sudo cp ${GEOSPARK_INSTALL_DIR}/sql/target/geospark-sql_3.0-1.3.2-SNAPSHOT.jar ${SPARK_HOME}/jars/
#sudo cp ${GEOSPARK_INSTALL_DIR}/viz/target/geospark-viz_3.0-1.3.2-SNAPSHOT.jar ${SPARK_HOME}/jars/  # currently incompatible

echo "-------------------------------------------------------------------------"
echo " Install OS & Python updates and packages"
echo "-------------------------------------------------------------------------"

sudo yum -y update
sudo yum -y install tmux  # to enable logging out of the remote server while running a long job

# update package installers
python -m pip install --user --upgrade pip
pip install --user --upgrade setuptools

# install AWS packages
pip install --user awscli
pip install --user boto3

# Install Apache Sedona package
cd ${GEOSPARK_INSTALL_DIR}/python || exit
python setup.py install --user
#pip install --user geospark  # for when the version on PyPi is updated to 1.3.2 (the Spark 3 compatible version)

echo "-------------------------------------------------------------------------"
echo " Setup Spark"
echo "-------------------------------------------------------------------------"

echo "JAVA_HOME=${JAVA_HOME}" | sudo tee /etc/environment
echo "SPARK_HOME=${SPARK_HOME}" | sudo tee -a /etc/environment
echo "HADOOP_HOME=${HADOOP_HOME}" | sudo tee -a /etc/environment

# reduce Spark logging to warnings and above (i.e no INFO or DEBUG messages)
sudo cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties
sudo sed -i -e "s/log4j.rootCategory=INFO, console/log4j.rootCategory=WARN, console/g" $SPARK_HOME/conf/log4j.properties

echo "----------------------------------------------------------------------------------------------------------------"

cd ~ || exit

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
