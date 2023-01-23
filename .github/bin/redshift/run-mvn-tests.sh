#!/usr/bin/env bash

# TODO
# TODO Warning: this is just a temporary version of the script, to be replaced.
# TODO It has not been tidied up and doesn't yet correspond to how we write scripts.
# TODO

# Runs Redshift connector tests.
#
# Run setup-aws-redshift.sh as a prerequisite for creating the Redshift instance and viewing
# required environment variables.
#
# Usage:
# run-mvn-tests.sh '-pl :trino-redshift'

set -xeuo pipefail

REDSHIFT_SCRIPTS_DIR="${BASH_SOURCE%/*}"
PROJECT_ROOT="${REDSHIFT_SCRIPTS_DIR}/../.."

cd "${PROJECT_ROOT}" || exit 1

suite_exit_code=0

${MAVEN} ${MAVEN_TEST}\
    test \
    -B -Dair.check.skip-all=true -Dmaven.javadoc.skip=true --fail-at-end \
    -Dtest.redshift.jdbc.user="${REDSHIFT_USER}" \
    -Dtest.redshift.jdbc.password="${REDSHIFT_PASSWORD}" \
    -Dtest.redshift.jdbc.endpoint="${REDSHIFT_ENDPOINT}:${REDSHIFT_PORT}/" \
    -Dtest.redshift.s3.tpch.tables.root="${REDSHIFT_S3_TPCH_TABLES_ROOT}" \
    -Dtest.redshift.iam.role="${REDSHIFT_IAM_ROLES}" \
    -Dtest.redshift.aws.region="${AWS_REGION}" \
    -Dtest.redshift.aws.access-key="${AWS_ACCESS_KEY_ID}" \
    -Dtest.redshift.aws.secret-key="${AWS_SECRET_ACCESS_KEY}" \
    "$@" ||
    suite_exit_code=1

echo "$0: exiting with ${suite_exit_code}"
exit "${suite_exit_code}"
