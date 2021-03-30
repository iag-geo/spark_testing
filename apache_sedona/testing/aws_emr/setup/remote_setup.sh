#!/usr/bin/env bash

SECONDS=0*

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"

echo "-------------------------------------------------------------------------"
echo " Set temp local environment vars"
echo "-------------------------------------------------------------------------"

POSTGRES_JDBC_VERSION="42.2.18"
#HADOOP_AWS_VERSION="3.2.1"
MAVEN_VERSION=3.6.3

SEDONA_INSTALL_DIR=~/incubator-sedona-1.3.2-spark-3.0

export no_proxy="<your no proxy server addresses>"
export http_proxy="http://<your proxy server address>"
export https_proxy=${http_proxy}
export HTTP_PROXY=${http_proxy}
export HTTPS_PROXY=${http_proxy}
export NO_PROXY=${no_proxy}

export PATH=/usr/lib/apache-maven-$MAVEN_VERSION/bin:$PATH

export JAVA_HOME="/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre"
export SPARK_HOME="/usr/lib/spark"
export HADOOP_HOME="/usr/lib/hadoop"

## get directory this script is running from
#SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "-------------------------------------------------------------------------"
echo "Install Maven"
echo "-------------------------------------------------------------------------"

## this is only required if behind a proxy or you need a custom Maven config
#mkdir ~/.m2
#
#cat <<EOF >~/.m2/settings.xml
#<settings>
#    <proxies>
#        <proxy>
#            <id>iag-non-prod-http</id>
#            <active>true</active>
#            <protocol>http</protocol>
#            <host>your proxy server address without the http://</host>
#            <port>your proxy server port</port>
#            <nonProxyHosts>your no proxy server addresses</nonProxyHosts>
#        </proxy>
#        <proxy>
#            <id>iag-non-prod-https</id>
#            <active>true</active>
#            <protocol>https</protocol>
#            <host>your proxy server address without the http://</host>
#            <port>your proxy server port</port>
#            <nonProxyHosts>your no proxy server addresses</nonProxyHosts>
#        </proxy>
#    </proxies>
#</settings>
#EOF

curl -O https://www.strategylions.com.au/mirror/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz
tar xzf apache-maven-$MAVEN_VERSION-bin.tar.gz
sudo mv apache-maven-$MAVEN_VERSION /usr/lib/apache-maven-$MAVEN_VERSION
rm apache-maven-$MAVEN_VERSION-bin.tar.gz

echo "-------------------------------------------------------------------------"
echo " Download Postgres JDBC file"
echo "-------------------------------------------------------------------------"

# download Postgres JDBC JAR
curl -O https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar
sudo mv postgresql-${POSTGRES_JDBC_VERSION}.jar ${SPARK_HOME}/jars/

echo "-------------------------------------------------------------------------"
echo " Build & install Apache Sedona"
echo "-------------------------------------------------------------------------"

# download Apache Sedona source code
curl -O https://github.com/apache/incubator-sedona/archive/1.3.2-spark-3.0.tar.gz
tar xzf 1.3.2-spark-3.0.tar.gz
rm 1.3.2-spark-3.0.tar.gz

## copy maven files from S3 for faster build process - only useful if Maven is taking forever
##     - need to copy files to your S3 bucket first
#mkdir -p ~/.m2/repository
#aws s3 sync --quiet s3://maven-downloads/sedona-1.3.2/repository ~/.m2/repository

# Build it
cd ${SEDONA_INSTALL_DIR} || exit
mvn clean install -DskipTests
#-Dmaven.wagon.http.ssl.insecure=true \  # only for corpoate networks that have weird proxies
#-Dmaven.wagon.http.ssl.allowall=true \
#-Dmaven.wagon.http.ssl.ignore.validity.dates=true

# Copy JARs to Spark folder
sudo cp ${SEDONA_INSTALL_DIR}/core/target/sedona-1.3.2-SNAPSHOT.jar ${SPARK_HOME}/jars/
sudo cp ${SEDONA_INSTALL_DIR}/sql/target/sedona-sql_3.0-1.3.2-SNAPSHOT.jar ${SPARK_HOME}/jars/
#sudo cp ${SEDONA_INSTALL_DIR}/viz/target/sedona-viz_3.0-1.3.2-SNAPSHOT.jar ${SPARK_HOME}/jars/  # currently incompatible with Spark 3

echo "-------------------------------------------------------------------------"
echo " Install OS & Python updates and packages"
echo "-------------------------------------------------------------------------"

sudo yum -q -y update
sudo yum -q -y install tmux  # to enable logging out of the remote server while running a long job

# update package installers
python -m pip install --user --upgrade pip
python -m pip install --user --upgrade setuptools

# install AWS packages
pip install --user awscli
pip install --user boto3

# Install Apache Sedona package
cd ${SEDONA_INSTALL_DIR}/python || exit
python setup.py install --user
#pip install --user sedona  # for when the version on PyPi is updated to 1.3.2 (the Spark 3 compatible version)

echo "-------------------------------------------------------------------------"
echo " Setup Spark"
echo "-------------------------------------------------------------------------"

echo "JAVA_HOME=${JAVA_HOME}" | sudo tee /etc/environment
echo "SPARK_HOME=${SPARK_HOME}" | sudo tee -a /etc/environment
echo "HADOOP_HOME=${HADOOP_HOME}" | sudo tee -a /etc/environment

# reduce Spark logging to warnings and above (i.e no INFO or DEBUG messages)
sudo cp $SPARK_HOME/conf/log4j.properties.template $SPARK_HOME/conf/log4j.properties
sudo sed -i -e "s/log4j.rootCategory=INFO, console/log4j.rootCategory=WARN, console/g" $SPARK_HOME/conf/log4j.properties

echo "-------------------------------------------------------------------------"
echo " Remove proxy"
echo "-------------------------------------------------------------------------"

unset http_proxy
unset HTTP_PROXY
unset https_proxy
unset HTTPS_PROXY
unset no_proxy
unset NO_PROXY

echo "----------------------------------------------------------------------------------------------------------------"

cd ~ || exit

duration=$SECONDS

echo " End time : $(date)"
echo " Apache Sedona install took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"
