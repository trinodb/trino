#!/bin/sh

set -eux

# create CA and pem truststore
openssl genrsa 4096 | openssl pkcs8 -v1 PBE-SHA1-3DES -topk8 -inform pem -outform pem -nocrypt -out ca.key
openssl req -new -x509 -days 9999 -subj "/C=US/ST=CA/L=Boston/O=Trino/OU=RootCA" -key ca.key -out ca.crt

# create jks truststore
keytool -import -noprompt -alias ca -file ca.crt -storetype pkcs12 -storepass changeit -keystore jks.truststore
