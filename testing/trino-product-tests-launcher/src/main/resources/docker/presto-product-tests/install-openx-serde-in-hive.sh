#!/bin/bash
set -xeuo pipefail
cp --no-clobber --verbose /docker/openx-serde.jar /usr/hdp/*/hive/lib
