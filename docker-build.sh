#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker build \
    -t quay.io/coreos/presto:metering-0.212 \
    -f "$DIR/Dockerfile" \
    "$DIR"
