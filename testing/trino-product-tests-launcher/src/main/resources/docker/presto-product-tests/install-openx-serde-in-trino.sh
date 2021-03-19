#!/bin/bash
set -xeuo pipefail
cp --no-clobber --verbose /docker/openx-serde.jar /docker/presto-server/plugin/hive-hadoop2
