#!/usr/bin/env bash

set -euo pipefail

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"

# Redshift requires passwords containing at least a digit, a lower case letter and a upper case letter.
# Having no warranty that openssl will output a string following the above mentioned password policy,
# add explicitly the string 'Red1!' to the password
REDSHIFT_PASSWORD="$(openssl rand -base64 16 | tr -dc 'a-zA-Z0-9')Red1!"

REDSHIFT_CLUSTER_IDENTIFIER=trino-redshift-ci-cluster-$(openssl rand -hex 8)

REDSHIFT_CLUSTER_TTL=$(date -u -d "+2 hours" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v "+2H" +"%Y-%m-%dT%H:%M:%SZ")

echo "Creating the Amazon Redshift cluster ${REDSHIFT_CLUSTER_IDENTIFIER} on the region ${AWS_REGION}."
REDSHIFT_CREATE_CLUSTER_OUTPUT=$(aws redshift create-cluster \
  --db-name testdb \
  --region ${AWS_REGION} \
  --node-type dc2.large \
  --number-of-nodes 1 \
  --master-username admin \
  --master-user-password ${REDSHIFT_PASSWORD} \
  --cluster-identifier ${REDSHIFT_CLUSTER_IDENTIFIER} \
  --cluster-subnet-group-name ${REDSHIFT_SUBNET_GROUP_NAME} \
  --cluster-type single-node\
  --vpc-security-group-ids "${REDSHIFT_VPC_SECURITY_GROUP_IDS}" \
  --iam-roles ${REDSHIFT_IAM_ROLES} \
  --automated-snapshot-retention-period 0 \
  --publicly-accessible \
  --tags Key=cloud,Value=aws Key=environment,Value=test Key=project,Value=trino-redshift Key=ttl,Value=${REDSHIFT_CLUSTER_TTL})

if [ -z "${REDSHIFT_CREATE_CLUSTER_OUTPUT}" ]; then
    # Only show errors
    echo ${REDSHIFT_CREATE_CLUSTER_OUTPUT}
    exit 1
fi

echo ${REDSHIFT_CLUSTER_IDENTIFIER} > ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier
echo "Waiting for the Amazon Redshift cluster ${REDSHIFT_CLUSTER_IDENTIFIER} on the region ${AWS_REGION} to be available."

# Wait for the cluster to become available
aws redshift wait cluster-available \
  --cluster-identifier ${REDSHIFT_CLUSTER_IDENTIFIER}

echo "The Amazon Redshift cluster ${REDSHIFT_CLUSTER_IDENTIFIER} on the region ${AWS_REGION} is available for queries."

REDSHIFT_CLUSTER_DESCRIPTION=$(aws redshift describe-clusters --cluster-identifier ${REDSHIFT_CLUSTER_IDENTIFIER})

export REDSHIFT_ENDPOINT=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].Endpoint.Address' )
export REDSHIFT_PORT=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].Endpoint.Port' )
export REDSHIFT_CLUSTER_DATABASE_NAME=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].DBName' )
export REDSHIFT_USER=$(echo ${REDSHIFT_CLUSTER_DESCRIPTION} | jq -r '.Clusters[0].MasterUsername' )
export REDSHIFT_PASSWORD
