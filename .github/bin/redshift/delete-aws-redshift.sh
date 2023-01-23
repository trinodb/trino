#!/usr/bin/env bash

set -euo pipefail

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"

if [[ ! -f "${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier" ]];  then
    echo "Missing file ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier"
    exit 0
fi

REDSHIFT_CLUSTER_IDENTIFIER=$(cat $REDSHIFT_SCRIPTS_DIR/.cluster-identifier)

echo "Deleting Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER"
aws redshift delete-cluster --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER --skip-final-cluster-snapshot

echo "Waiting for the Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER to be deleted"
aws redshift wait cluster-deleted \
  --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER
echo "Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER has been deleted"

rm -f ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier
exit 0
