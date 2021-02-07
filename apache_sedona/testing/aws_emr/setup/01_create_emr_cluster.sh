#!/usr/bin/env bash

SECONDS=0*

# --------------------------------------------------------------------------------------------------------------------
# Creates an AWS EMR cluster with Spark; ready for Apache Sedona (Geospark) to be installed
# --------------------------------------------------------------------------------------------------------------------
#
# Author: Hugh Saalmans, IAG Strategy & Innovation
# Date: 2020-11-13
#
# WARNINGS:
#   - Runs on MacOS and Linux only
#   - You will incur AWS costs once the EMR cluster has started
#        - The default settings below will cost you ~$1.50 per hour
#   - Uses SSH connections over AWS SSM
#        - If that's not configured or you don't want to use it - you need to 'fix' all SSH & SCP commands
#   - Default settings use spot instance pricing - change if you need to run an EMR Cluster long term
#
# PRE_REQUISITES:
#   1. AWS CLI is installed
#        - Install using Python pip: pip install awscli
#
#   2. You have an AWS EC2 private key (PEM) file
#        - Create one using the AWS EC2 Console
#
#   3. You have an AWS S3 bucket already created for the log files
#        - Create one using the AWS S3 Console
#
# ISSUES:
#   1. Not sure if EMR roles get created automatically if they don't exist
#
# --------------------------------------------------------------------------------------------------------------------
#
# EDIT YOUR SETUP:

SSH_CONFIG_FILE="${HOME}/.ssh/aws-emr-config"
KEYPAIR_NAME="<your EC2 keypair name>"
LOGFILE_S3_BUCKET="<your EMR log file S3 bucket name>"
EC2_INSTANCE_TYPE="m5.xlarge"
NUMBER_OF_CORE_SERVERS=4

# --------------------------------------------------------------------------------------------------------------------

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"

echo "-------------------------------------------------------------------------"
echo " Set temp local environment vars"
echo "-------------------------------------------------------------------------"

# get directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "-------------------------------------------------------------------------"
echo " Create EMR cluster, wait for startup and get cluster & master server IDs"
echo "-------------------------------------------------------------------------"

CLUSTER_ID=$(aws emr create-cluster \
--termination-protected \
--applications Name=Hadoop Name=Hive Name=Hue Name=Spark Name=JupyterHub \
--ec2-attributes '{"KeyName":"${KEYPAIR_NAME}","InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-0b55cd16d5595217c","SubnetId":"subnet-00e994fe9f63d9dbd","EmrManagedSlaveSecurityGroup":"sg-027b04b3dc9114d1c","EmrManagedMasterSecurityGroup":"sg-01e12be997b4a697a"}' \
--release-label emr-6.2.0 \
--log-uri 's3n://${LOGFILE_S3_BUCKET}/elasticmapreduce/' \
--instance-groups '[{"InstanceCount":${NUMBER_OF_CORE_SERVERS},"BidPrice":"OnDemandPrice","InstanceGroupType":"CORE","InstanceType":"${EC2_INSTANCE_TYPE}","Name":"Core - 2"},{"InstanceCount":1,"BidPrice":"OnDemandPrice","InstanceGroupType":"MASTER","InstanceType":"${EC2_INSTANCE_TYPE}","Name":"Master - 1"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}}]' \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--ebs-root-volume-size 100 \
--service-role EMR_DefaultRole \
--enable-debugging \
--name 'compass_iot_processing' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region ap-southeast-2 \
--no-verify-ssl | \
python3 -c "import sys, json; print(json.load(sys.stdin)['ClusterId'])")

# wait for cluster to startup
aws emr wait cluster-running --cluster-id ${CLUSTER_ID}

# get EC2 instance id & IP address of master server
INSTANCE_GROUP_ID=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} | \
python3 -c "import sys, json; print([i for i in json.load(sys.stdin)['Cluster']['InstanceGroups'] if i['InstanceGroupType'] == 'MASTER'][0]['Id'])")

INSTANCE_ID=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --instance-group-id ${INSTANCE_GROUP_ID} | \
python3 -c "import sys, json; print(json.load(sys.stdin)['Instances'][0]['Ec2InstanceId'])")

INSTANCE_IP_ADDRESS=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --instance-group-id ${INSTANCE_GROUP_ID} | \
python3 -c "import sys, json; print(json.load(sys.stdin)['Instances'][0]['PrivateIpAddress'])")

echo "-------------------------------------------------------------------------"
echo " Copy setup script for master server"
echo "-------------------------------------------------------------------------"

# also get rid of fingerprint check on trusted server
scp -F ${SSH_CONFIG_FILE} -o StrictHostKeyChecking=no ${SCRIPT_DIR}/remote_setup.sh hadoop@${INSTANCE_ID}:~/

echo "-------------------------------------------------------------------------"
echo " Copy python & sql scripts"
echo "-------------------------------------------------------------------------"

scp -F ${SSH_CONFIG_FILE} ${SCRIPT_DIR}/../*/*.py hadoop@${INSTANCE_ID}:~/
scp -F ${SSH_CONFIG_FILE} ${SCRIPT_DIR}/../*/*.sql hadoop@${INSTANCE_ID}:~/

echo "----------------------------------------------------------------------------------------------------------------"

# port forward Spark UI web site
ssh -F ${SSH_CONFIG_FILE} -fNL 4040:${INSTANCE_IP_ADDRESS}:18080 ${INSTANCE_ID}

duration=$SECONDS

echo " End time : $(date)"
echo " EMR cluster build took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"

echo "Cluster ID  : ${CLUSTER_ID}"
echo "Instance ID : ${INSTANCE_ID}"
echo "Instance IP : ${INSTANCE_IP_ADDRESS}"

# save vars to local file for use in case you close Terminal accidentally
echo "export SCRIPT_DIR=${SCRIPT_DIR}" > ~/git/temp_master_server_vars.sh
echo "export SSH_CONFIG_FILE=${SSH_CONFIG_FILE}" >> ~/git/temp_master_server_vars.sh
echo "export CLUSTER_ID=${CLUSTER_ID}" >> ~/git/temp_master_server_vars.sh
echo "export INSTANCE_ID=${INSTANCE_ID}" >> ~/git/temp_master_server_vars.sh
echo "export INSTANCE_IP_ADDRESS=${INSTANCE_IP_ADDRESS}" >> ~/git/temp_master_server_vars.sh

echo "----------------------------------------------------------------------------------------------------------------"

# connect to the EMR instance
ssh -F ${SSH_CONFIG_FILE} ${INSTANCE_ID}


# USEFUL COMMANDS
#
## connect to master server
# ssh -F ${SSH_CONFIG_FILE} ${INSTANCE_ID}
#
## copy python scripts to remote
# scp -F ${SSH_CONFIG_FILE} ${SCRIPT_DIR}/../*.py hadoop@${INSTANCE_ID}:~/
#
## port forward Spark UI web site (view it at http://localhost:4040)
# ssh -F ${SSH_EC2_USER_CONFIG} -fNL 4040:${INSTANCE_IP_ADDRESS}:18080 ${INSTANCE_ID}
#
## shutdown cluster
# aws emr terminate-clusters --cluster-ids ${CLUSTER_ID}

