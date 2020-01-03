#!/bin/bash

set -euo pipefail

# Create a dedicated profile for this action to avoid conflicts with past/future actions.
aws configure --profile upload_artifacts <<-EOF > /dev/null 2>&1
${INPUT_AWS_ACCESS_KEY_ID}
${INPUT_AWS_SECRET_ACCESS_KEY}
${INPUT_S3_BUCKET_REGION}
text
EOF

function cleanup {
    # Clear out credentials after we're done.
    # We need to re-run `aws configure` with bogus input instead of
    # deleting ~/.aws in case there are other credentials living there.
    # https://forums.aws.amazon.com/thread.jspa?threadID=148833
    aws configure --profile upload_artifacts <<-EOF > /dev/null 2>&1
null
null
null
text
EOF
}

trap cleanup EXIT

function s3_sync {
    aws s3 sync "$1" "s3://${INPUT_S3_BUCKET}/${INPUT_SHA}/" \
        --profile upload_artifacts \
        --no-progress \
        --endpoint-url ${INPUT_S3_BUCKET_ENDPOINT}
}

s3_sync **/target/*-reports
s3_sync **/target/*-reports/*
