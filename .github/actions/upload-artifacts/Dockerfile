FROM python:3.8-alpine

# https://github.com/aws/aws-cli/blob/master/CHANGELOG.rst
ENV AWSCLI_VERSION='1.16.310'

RUN pip install --quiet --no-cache-dir awscli==${AWSCLI_VERSION}

ADD upload-artifacts.sh /upload-artifacts.sh
ENTRYPOINT ["/upload-artifacts.sh"]
