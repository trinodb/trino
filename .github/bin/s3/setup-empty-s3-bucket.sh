#!/usr/bin/env bash

set -euo pipefail

S3_SCRIPTS_DIR="${BASH_SOURCE%/*}"

S3_BUCKET_IDENTIFIER=trino-s3fs-ci-$(openssl rand -hex 8)

# Support both -d and -v date offset formats depending on operating system (-d for linux, -v for osx)
S3_BUCKET_TTL=$(date -u -d "+2 hours" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v "+2H" +"%Y-%m-%dT%H:%M:%SZ")

echo "Creating an empty AWS S3 bucket ${S3_BUCKET_IDENTIFIER} in the region ${AWS_REGION}"

OPTIONAL_BUCKET_CONFIGURATION=()
# LocationConstraint configuration property is not allowed for us-east-1 AWS region
if [ "${AWS_REGION}" != 'us-east-1' ]; then
    OPTIONAL_BUCKET_CONFIGURATION+=("--create-bucket-configuration" "LocationConstraint=${AWS_REGION}")
fi

S3_CREATE_BUCKET_OUTPUT=$(aws s3api create-bucket \
  --bucket "${S3_BUCKET_IDENTIFIER}" \
  --region "${AWS_REGION}" \
  "${OPTIONAL_BUCKET_CONFIGURATION[@]}")

if [ -z "${S3_CREATE_BUCKET_OUTPUT}" ]; then
    echo "Unexpected error while attempting to create the S3 bucket ${S3_BUCKET_IDENTIFIER} in the region ${AWS_REGION}"
    exit 1
fi

echo "${S3_BUCKET_IDENTIFIER}" > "${S3_SCRIPTS_DIR}"/.bucket-identifier
echo "Waiting for the AWS S3 bucket ${S3_BUCKET_IDENTIFIER} in the region ${AWS_REGION} to exist"

# Wait for the bucket to exist
aws s3api wait bucket-exists \
  --bucket "${S3_BUCKET_IDENTIFIER}"

echo "The AWS S3 bucket ${S3_BUCKET_IDENTIFIER} in the region ${AWS_REGION} exists"

echo "Tagging the AWS S3 bucket ${S3_BUCKET_IDENTIFIER} with TTL tags"

# "test" environment tag is needed so that the bucket gets cleaned up by the daily AWS resource cleanup job in case the
# temporary bucket is not properly cleaned up by delete-s3-bucket.sh. The ttl tag tells the AWS resource cleanup job
# when the bucket is expired and should be cleaned up
aws s3api put-bucket-tagging \
  --bucket "${S3_BUCKET_IDENTIFIER}" \
  --tagging "TagSet=[{Key=environment,Value=test},{Key=ttl,Value=${S3_BUCKET_TTL}}]"
