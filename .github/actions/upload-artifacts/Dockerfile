FROM python:3.8-slim

# https://github.com/aws/aws-cli/blob/master/CHANGELOG.rst
ENV AWSCLI_VERSION='1.16.310'
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG S3_BUCKET
ARG S3_BUCKET_REGION
ARG S3_BUCKET_ENDPOINT
ARG SHA

RUN pip install --quiet --no-cache-dir awscli==${AWSCLI_VERSION}

ADD upload-artifacts.sh /upload-artifacts.sh
ENTRYPOINT ["/upload-artifacts.sh"]
