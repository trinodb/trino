#!/bin/bash
set -xeuo pipefail
cp --no-clobber --verbose /docker/kafka-protobuf-provider/* /docker/presto-server/plugin/kafka
