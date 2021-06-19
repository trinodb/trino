#!/usr/bin/env bash

set -euo pipefail

. "${BASH_SOURCE%/*}/common.sh"

cleanup_hadoop_docker_containers
start_hadoop_docker_containers

HADOOP_MASTER_IP=$(hadoop_master_ip)

# get short version of container ID (as shown by "docker ps")
CONTAINER=$(echo "${HADOOP_MASTER_CONTAINER}" | cut -b1-12)

echo
echo "Proxy: ${PROXY}:1180"
echo "Hadoop: ${HADOOP_MASTER_IP}"
echo "Docker: ${CONTAINER}"
echo
echo "docker exec -it ${CONTAINER} bash"
echo
