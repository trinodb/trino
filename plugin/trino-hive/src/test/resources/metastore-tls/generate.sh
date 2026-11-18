#!/usr/bin/env bash
# Regenerates the TLS material in this directory used by TestTransport.

set -euo pipefail

DAYS=36500

openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days $DAYS -subj "/CN=Trino Metastore Test CA" -out ca.crt

# server certificate matching the host the client connects to
openssl genrsa -out localhost.key 2048
openssl req -new -key localhost.key -subj "/CN=localhost" -out localhost.csr
openssl x509 -req -in localhost.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $DAYS -sha256 \
    -extfile <(printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n') -out localhost.crt
cat localhost.key localhost.crt > localhost.pem

# server certificate from the same CA, issued for a different host
openssl genrsa -out differenthost.key 2048
openssl req -new -key differenthost.key -subj "/CN=differenthost.local" -out differenthost.csr
openssl x509 -req -in differenthost.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $DAYS -sha256 \
    -extfile <(printf 'subjectAltName=DNS:differenthost.local\nextendedKeyUsage=serverAuth\n') -out differenthost.crt
cat differenthost.key differenthost.crt > differenthost.pem

rm -f ca.key ca.srl localhost.key localhost.crt localhost.csr differenthost.key differenthost.crt differenthost.csr
