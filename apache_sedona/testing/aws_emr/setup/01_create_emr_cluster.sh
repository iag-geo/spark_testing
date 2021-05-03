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

echo "-------------------------------------------------------------------------"
echo " Set temp local environment vars"
echo "-------------------------------------------------------------------------"

SSH_CONFIG="${HOME}/.ssh/aws-sandbox-emr-config"

# get directory this script is running from
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# load AWS parameters
. ${HOME}/.aws/ec2_vars.sh

echo "-------------------------------------------------------------------------"
echo " Copy bootstrap bash file to S3 (adds additional Python packages to EMR)"
echo "-------------------------------------------------------------------------"

aws s3 cp ${SCRIPT_DIR}/emr_bootstrap.sh s3://${AWS_S3_BUCKET}/emr_bootstrap.sh

echo "-------------------------------------------------------------------------"
echo " Create EMR cluster, wait for startup and get cluster & master server IDs"
echo "-------------------------------------------------------------------------"

INSTANCE_TYPE="r5d.2xlarge"
INSTANCE_COUNT=4

EC2_ATTRIBUTES="{\"KeyName\":\"${AWS_KEYPAIR}\",\"InstanceProfile\":\"EMR_EC2_DefaultRole\",\"ServiceAccessSecurityGroup\":\"${AWS_EMR_SERVICE_SG}\",\"SubnetId\":\"${AWS_SUBNET}\",\"EmrManagedSlaveSecurityGroup\":\"${AWS_EMR_NODE_SG}\",\"EmrManagedMasterSecurityGroup\":\"${AWS_EMR_MASTER_SG}\"}"
INSTANCE_GROUPS="[{\"InstanceCount\":${INSTANCE_COUNT},\"BidPrice\":\"OnDemandPrice\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${INSTANCE_TYPE}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"BidPrice\":\"OnDemandPrice\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${INSTANCE_TYPE}\",\"Name\":\"Master - 1\"}]"
#echo "${EC2_ATTRIBUTES}"
#echo ""
#echo "${INSTANCE_GROUPS}"
#echo ""

CLUSTER_ID=$(aws emr create-cluster \
--termination-protected \
--applications Name=Hadoop Name=Hive Name=Spark \
--ec2-attributes "${EC2_ATTRIBUTES}" \
--release-label emr-6.2.0 \
--log-uri "s3n://${AWS_S3_BUCKET}/elasticmapreducelogs/" \
--instance-groups "${INSTANCE_GROUPS}" \
--configurations "[{\"Classification\":\"spark\",\"Properties\":{\"maximizeResourceAllocation\":\"true\"}}]" \
--bootstrap-actions Path="s3://${AWS_S3_BUCKET}/emr_bootstrap.sh" \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--ebs-root-volume-size 100 \
--service-role EMR_DefaultRole \
--enable-debugging \
--name "mobility_ai_processing" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region ap-southeast-2 \
--no-verify-ssl | \
python3 -c "import sys, json; print(json.load(sys.stdin)['ClusterId'])")

#--bootstrap-actions Path="s3://elasticmapreduce/bootstrap-actions/run-if", Args=["instance.isMaster=true","echo running on master node"], Path="s3://mobai-sandpit-bucket-compassiot-oem/emr_setup/remote_setup.sh", Name="remote_setup_script" \

# SP's test custom AMI
#--custom-ami-id ami-00851a17767e02d95 \

# wait for it to startup
aws emr wait cluster-running --cluster-id ${CLUSTER_ID}

# get instance id of master server
INSTANCE_GROUP_ID=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} | \
python3 -c "import sys, json; print([i for i in json.load(sys.stdin)['Cluster']['InstanceGroups'] if i['InstanceGroupType'] == 'MASTER'][0]['Id'])")

INSTANCE_ID=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --instance-group-id ${INSTANCE_GROUP_ID} | \
python3 -c "import sys, json; print(json.load(sys.stdin)['Instances'][0]['Ec2InstanceId'])")

INSTANCE_IP_ADDRESS=$(aws emr list-instances --cluster-id ${CLUSTER_ID} --instance-group-id ${INSTANCE_GROUP_ID} | \
python3 -c "import sys, json; print(json.load(sys.stdin)['Instances'][0]['PublicIpAddress'])")

echo "-------------------------------------------------------------------------"
echo " Copy setup script for master server and run it remotely"
echo "-------------------------------------------------------------------------"

# also get rid of fingerprint check on trusted server
scp -i ${AWS_KEYPAIR} -o StrictHostKeyChecking=no ${SCRIPT_DIR}/remote_setup.sh hadoop@${INSTANCE_IP_ADDRESS}:~/

# run remote setup script
ssh -i ${AWS_KEYPAIR} hadoop@${INSTANCE_IP_ADDRESS} ". ./remote_setup.sh"

echo "-------------------------------------------------------------------------"
echo " Copy python & sql scripts"
echo "-------------------------------------------------------------------------"

scp -i ${AWS_KEYPAIR} ${SCRIPT_DIR}/../*/*.py hadoop@${INSTANCE_IP_ADDRESS}:~/
scp -i ${AWS_KEYPAIR} ${SCRIPT_DIR}/../*/*.sql hadoop@${INSTANCE_IP_ADDRESS}:~/

echo "----------------------------------------------------------------------------------------------------------------"

# port forward Spark UI web site
ssh -i ${AWS_KEYPAIR} -fNL 4040:${INSTANCE_IP_ADDRESS}:18080 ${INSTANCE_IP_ADDRESS}

## port forward Jupyter notebook
#ssh -i ${AWS_KEYPAIR} -fNL 8889:${INSTANCE_IP_ADDRESS}:8889 ${INSTANCE_IP_ADDRESS}

duration=$SECONDS

echo " End time : $(date)"
echo " EMR cluster build took $((duration / 60)) mins"
echo "----------------------------------------------------------------------------------------------------------------"

echo "Cluster ID  : ${CLUSTER_ID}"
echo "Instance ID : ${INSTANCE_IP_ADDRESS}"
echo "Instance IP : ${INSTANCE_IP_ADDRESS}"

# save vars to local file
echo "export SCRIPT_DIR=${SCRIPT_DIR}" > ~/git/temp_master_server_vars.sh
echo "export SSH_CONFIG=${SSH_CONFIG}" >> ~/git/temp_master_server_vars.sh
echo "export CLUSTER_ID=${CLUSTER_ID}" >> ~/git/temp_master_server_vars.sh
echo "export INSTANCE_ID=${INSTANCE_ID}" >> ~/git/temp_master_server_vars.sh
echo "export INSTANCE_IP_ADDRESS=${INSTANCE_IP_ADDRESS}" >> ~/git/temp_master_server_vars.sh

echo "----------------------------------------------------------------------------------------------------------------"

## load Python env to enable copying files from S3 to local Postgres
#conda activate minus34

# USEFUL COMMANDS
#
## connect to master server
# ssh -i ${AWS_KEYPAIR} ${INSTANCE_IP_ADDRESS}
#
## copy python scripts to remote
# scp -i ${AWS_KEYPAIR} ${SCRIPT_DIR}/../*.py hadoop@${INSTANCE_IP_ADDRESS}:~/
#
## port forward Spark UI web site (view it at http://localhost:4040)
# ssh -F ${SSH_EC2_USER_CONFIG} -fNL 4040:${INSTANCE_IP_ADDRESS}:18080 ${INSTANCE_IP_ADDRESS}
#
## shutdown cluster
# aws emr terminate-clusters --cluster-ids ${CLUSTER_ID}

