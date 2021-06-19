#!/bin/bash

set -xeuo pipefail

# Hadoop 3 without -d (don't create _COPYING_ temporary file) requires additional S3 permissions
# Hadoop 2 doesn't have '-d' switch
hadoop fs -put -f -d "$@" ||
hadoop fs -put -f "$@"
