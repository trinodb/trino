#!/usr/bin/env bash

set -euo pipefail

S3_SCRIPTS_DIR="${BASH_SOURCE%/*}"

if [[ ! -f "${S3_SCRIPTS_DIR}/.bucket-identifier" ]];  then
    echo "Missing file ${S3_SCRIPTS_DIR}/.bucket-identifier"
    exit 1
fi

S3_BUCKET_IDENTIFIER=$(cat "${S3_SCRIPTS_DIR}"/.bucket-identifier)

echo "Deleting all leftover objects from AWS S3 bucket ${S3_BUCKET_IDENTIFIER}"
aws s3 rm s3://"${S3_BUCKET_IDENTIFIER}" \
  --region "${AWS_REGION}" \
  --recursive

echo "Deleting AWS S3 bucket ${S3_BUCKET_IDENTIFIER}"
aws s3api delete-bucket \
  --bucket "${S3_BUCKET_IDENTIFIER}" \
  --region "${AWS_REGION}"

echo "Waiting for AWS S3 bucket ${S3_BUCKET_IDENTIFIER} to be deleted"

aws s3api wait bucket-not-exists \
  --bucket "${S3_BUCKET_IDENTIFIER}" \
  --region "${AWS_REGION}"

echo "AWS S3 bucket ${S3_BUCKET_IDENTIFIER} has been deleted"

rm -f "${S3_SCRIPTS_DIR}"/.bucket-identifier
