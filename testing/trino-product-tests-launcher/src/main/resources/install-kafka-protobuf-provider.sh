#!/bin/bash
set -xeuo pipefail
cp --no-clobber --verbose /docker/kafka-protobuf-provider/* /docker/trino-server/plugin/kafka
