#!/bin/bash

set -exuo pipefail

cd "${BASH_SOURCE%/*}/../.."

presto-hive-hadoop2/bin/run_hive_tests.sh

if [[ -v HIVE_TESTS_AWS_ACCESS_KEY_ID ]]; then
  env AWS_ACCESS_KEY_ID="${HIVE_TESTS_AWS_ACCESS_KEY_ID}" \
    AWS_SECRET_ACCESS_KEY="${HIVE_TESTS_AWS_SECRET_ACCESS_KEY}" \
    S3_BUCKET_ENDPOINT="${S3_TESTS_BUCKET_ENDPOINT}" \
    S3_BUCKET="${S3_TESTS_BUCKET}" \
    presto-hive-hadoop2/bin/run_hive_s3_tests.sh
fi

if [[ -v HIVE_TESTS_AWS_ACCESS_KEY_ID ]]; then
    env AWS_ACCESS_KEY_ID="${HIVE_TESTS_AWS_ACCESS_KEY_ID}" \
        AWS_SECRET_ACCESS_KEY="${HIVE_TESTS_AWS_SECRET_ACCESS_KEY}" \
        ./mvnw test $MAVEN_SKIP_CHECKS_AND_DOCS -B -pl presto-hive -P test-hive-glue
fi
