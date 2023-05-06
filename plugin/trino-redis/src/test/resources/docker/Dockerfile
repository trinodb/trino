FROM redis:7

RUN apt-get update && apt-get install openssl

WORKDIR /

COPY ./gen-test-certs.sh .

RUN chmod +x ./gen-test-certs.sh

RUN ./gen-test-certs.sh

RUN chown -R redis:redis /tests

WORKDIR /data
