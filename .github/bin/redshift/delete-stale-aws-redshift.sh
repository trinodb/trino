#!/usr/bin/env bash

set -uo pipefail

delete_cluster() {
    local cluster_id="$1"
    echo "Deleting Amazon Redshift cluster ${cluster_id}"
    local output
    output=$(aws redshift delete-cluster --cluster-identifier "${cluster_id}" --skip-final-cluster-snapshot)
    if [ -z "${output}" ]; then
        echo "${output}"
        # Don't fail the build because of cleanup issues
        return 0
    fi
    echo "Waiting for the Amazon Redshift cluster ${cluster_id} to be deleted"
    aws redshift wait cluster-deleted \
      --cluster-identifier "${cluster_id}"
    if [ "$?" -ne 0 ]; then
        echo "Amazon Redshift cluster ${cluster_id} deletion has timed out"
    else
        echo "Amazon Redshift cluster ${cluster_id} has been deleted"
    fi
}

# 60m comes from 'test' job in ci.yml
echo "Checking for stale Amazon Redshift clusters older than 60 minutes..."
CUTOFF_EPOCH=$(( $(date -u +%s) - 3600 ))
STALE_CLUSTERS=$(aws redshift describe-clusters \
    --query "Clusters[?Tags[?Key=='project' && Value=='trino-redshift']].{Id:ClusterIdentifier,Created:ClusterCreateTime}" \
    --output json \
  | jq -r --argjson cutoff "${CUTOFF_EPOCH}" \
    '.[] | select((.Created | sub("\\.[0-9]+\\+.*$"; "Z") | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime) < $cutoff) | .Id')
for stale_cluster_id in ${STALE_CLUSTERS}; do
    delete_cluster "${stale_cluster_id}"
done

exit 0
