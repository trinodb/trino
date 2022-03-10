#!/usr/bin/env bash

DEFAULT_DOCKER_VERSION=$(./mvnw help:evaluate -Dexpression=dep.docker.images.version -q -DforceStdout)

if [ -z "$DEFAULT_DOCKER_VERSION" ];
then
     >&2 echo "Could not read dep.docker.images.version from parent POM"
    exit 1
fi

export DOCKER_IMAGES_VERSION=${DOCKER_IMAGES_VERSION:-$DEFAULT_DOCKER_VERSION}
export HADOOP_BASE_IMAGE="${HADOOP_BASE_IMAGE:-ghcr.io/trinodb/testing/hdp2.6-hive}"
