#!/usr/bin/env bash

SECONDS=0*

echo "-------------------------------------------------------------------------"
echo " Start time : $(date)"
echo "-------------------------------------------------------------------------"

# --------------------------------------------------------------------------------------------------------------------
# Script creates an AWS EMR cluster with Spark 3 & Hadoop 3; with Apache Sedona (Geospark) installed on top
#
# This is step 1 of 2; Step 2 is to run "remote_setup.sh" after connecting to the EMR master server
#
# --------------------------------------------------------------------------------------------------------------------
#
# Author: Hugh Saalmans, IAG Strategy & Innovation
# Date: 2020-11-24
#
# PRE_REQUISITES:
#   1. Your AWS credentials are stored locally in the default file (e.g. ~/.aws/credentials on a Mac)
#
#   2. You've created a key pair in the AWS EC2 Console and copied the private key (PEM) file locally
#
#   3. Currently uses SSH connections over AWS Session Manager (SSM). A future version will be vanilla SSH (less secure)
#
# ISSUES:
#   1. Spark 3 requires Apache Sedona v1.3.2. As at 28/11/2020 v1.3.2 install needs to be built from source code
#
# --------------------------------------------------------------------------------------------------------------------
#
# SETUP:
#   - edit these if its now the future and versions have changed
#

SSH_HADOOP_USER_CONFIG="${HOME}/.ssh/aws-sandbox-emr-config"
SSH_EC2_USER_CONFIG="${HOME}/.ssh/aws-sandbox-config"
KEY_NAME="minus34"
PEM_FILE="${HOME}/.ssh/aws/${KEY_NAME}.pem"

# get directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "-------------------------------------------------------------------------"
echo " Create EMR cluster, wait for startup and get cluster & master server IDs"
echo "-------------------------------------------------------------------------"

CLUSTER_ID=$(aws emr create-cluster \
--termination-protected \
--applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark Name=JupyterHub \
--ec2-attributes '{"KeyName":"${KEY_NAME}","InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-0b55cd16d5595217c","SubnetId":"subnet-00e994fe9f63d9dbd","EmrManagedSlaveSecurityGroup":"sg-027b04b3dc9114d1c","EmrManagedMasterSecurityGroup":"sg-01e12be997b4a697a"}' \
--release-label emr-6.1.0 \
--log-uri 's3n://aws-logs-534411382636-ap-southeast-2/elasticmapreduce/' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core - 2"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}}]' \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--ebs-root-volume-size 100 \
--service-role EMR_DefaultRole \
--enable-debugging \
--name 'apache_sedona_testing' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region ap-southeast-2 | \
python3 -c "import sys, json; print(json.load(sys.stdin)['ClusterId'])")

#--no-verify-ssl | \

# SP's test custom AMI
#--custom-ami-id ami-00851a17767e02d95 \

# wait for it to startup
aws emr wait cluster-running --cluster-id ${CLUSTER_ID}

#CLUSTER_ID=j-A259L480DCED

# get instance id of master server
INSTANCE_GROUP_ID=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} | \
python3 -c "import sys, json; print([i for i in json.load(sys.stdin)['Cluster']['InstanceGroups'] if i['InstanceGroupType'] == 'MASTER'][0]['Id'])")

INSTANCE_ID=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --instance-group-id ${INSTANCE_GROUP_ID} | \
python3 -c "import sys, json; print(json.load(sys.stdin)['Instances'][0]['Ec2InstanceId'])")

INSTANCE_IP_ADDRESS=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --instance-group-id ${INSTANCE_GROUP_ID} | \
python3 -c "import sys, json; print(json.load(sys.stdin)['Instances'][0]['PrivateIpAddress'])")

echo "-------------------------------------------------------------------------"
echo " Copy AWS creds to master server to access S3 from Spark"
echo "-------------------------------------------------------------------------"

# get rid of fingerprint check on trusted server
scp -i ${PEM_FILE} -o StrictHostKeyChecking=no -r ${HOME}/.aws/credentials hadoop@${INSTANCE_ID}:~/.aws/credentials

echo "-------------------------------------------------------------------------"
echo " Copy setup script"
echo "-------------------------------------------------------------------------"

scp -i ${PEM_FILE} ${SCRIPT_DIR}/remote_setup.sh hadoop@${INSTANCE_ID}:~/

echo "-------------------------------------------------------------------------"
echo " Copy python scripts"
echo "-------------------------------------------------------------------------"

scp -i ${PEM_FILE} ${SCRIPT_DIR}/../*.py hadoop@${INSTANCE_ID}:~/

echo "----------------------------------------------------------------------------------------------------------------"

# port forward Spark UI web site (URL will be http://localhost:4040)
ssh -F ${SSH_EC2_USER_CONFIG} -fNL 4040:${INSTANCE_IP_ADDRESS}:18080 ${INSTANCE_ID}

duration=$SECONDS

echo " End time : $(date)"
echo " it took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"

echo "Cluster ID  : ${CLUSTER_ID}"
echo "Instance ID : ${INSTANCE_ID}"
echo "Instance IP : ${INSTANCE_IP_ADDRESS}"

# save vars to file in cae you ned them later...
echo "export CLUSTER_ID=${CLUSTER_ID}" > ${SCRIPT_DIR}/temp_master_server_vars.sh
echo "export INSTANCE_ID=${INSTANCE_ID}" >> ${SCRIPT_DIR}/temp_master_server_vars.sh
echo "export INSTANCE_IP_ADDRESS=${INSTANCE_IP_ADDRESS}" >> ${SCRIPT_DIR}/temp_master_server_vars.sh

echo "----------------------------------------------------------------------------------------------------------------"

### connect to the EMR instance
##ssh -F ${SSH_HADOOP_USER_CONFIG} ${INSTANCE_ID}
#
##aws emr terminate-clusters --cluster-ids ${CLUSTER_ID}

