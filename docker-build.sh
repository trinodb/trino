#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build \
    -t quay.io/openshift/origin-metering-presto:latest \
    -f "$DIR/Dockerfile" \
    "$DIR"
