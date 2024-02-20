#!/usr/bin/env bash

set -uo pipefail

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"

if [[ ! -f "${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier" ]];  then
    echo "Missing file ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier"
    exit 0
fi

REDSHIFT_CLUSTER_IDENTIFIER=$(cat $REDSHIFT_SCRIPTS_DIR/.cluster-identifier)

echo "Deleting Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER"
REDSHIFT_DELETE_CLUSTER_OUTPUT=$(aws redshift delete-cluster --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER --skip-final-cluster-snapshot)

if [ -z "${REDSHIFT_DELETE_CLUSTER_OUTPUT}" ]; then
    echo ${REDSHIFT_DELETE_CLUSTER_OUTPUT}
    # Don't fail the build because of cleanup issues
    exit 0
fi

echo "Waiting for the Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER to be deleted"
aws redshift wait cluster-deleted \
  --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER
if [ "$?" -ne 0 ]
then
  echo "Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER deletion has timed out"
else
  echo "Amazon Redshift cluster $REDSHIFT_CLUSTER_IDENTIFIER has been deleted"
fi

rm -f ${REDSHIFT_SCRIPTS_DIR}/.cluster-identifier
exit 0
