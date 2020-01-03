FROM python:3.8-slim

# https://github.com/aws/aws-cli/blob/master/CHANGELOG.rst
ENV AWSCLI_VERSION='1.16.310'
ENV AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY
ENV S3_BUCKET
ENV S3_BUCKET_REGION
ENV S3_BUCKET_ENDPOINT
ENV SHA

RUN pip install --quiet --no-cache-dir awscli==${AWSCLI_VERSION}

ADD upload-artifacts.sh /upload-artifacts.sh
ENTRYPOINT ["/upload-artifacts.sh"]
