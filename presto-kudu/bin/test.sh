#!/bin/bash

set -exuo pipefail

cd "${BASH_SOURCE%/*}/../.."

presto-kudu/bin/run_kudu_tests.sh 3 null
presto-kudu/bin/run_kudu_tests.sh 1 ""
presto-kudu/bin/run_kudu_tests.sh 1 presto::
