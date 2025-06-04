#!/usr/bin/env bash

set -xeuo pipefail

# Generate CA key
openssl req \
    -new \
    -x509 \
    -keyout ca-1.key \
    -out ca-1.crt \
    -days 9999 \
    -subj '/CN=kafka/OU=TEST/O=TRINO/L=Montreal/S=Qc/C=CA' \
    -addext "subjectAltName = DNS:kafka" \
    -passin pass:confluent \
    -passout pass:confluent

# Verify cert
# openssl x509 -in ca-1.crt -text -noout

for i in broker1 client; do
    echo $i
    # Create keystores
    keytool -genkey -noprompt \
        -alias $i \
        -dname "CN=kafka, OU=TEST, O=TRINO, L=Montreal, S=Qc, C=CA" \
        -keystore kafka.$i.keystore \
        -keyalg RSA \
        -storepass confluent \
        -keypass confluent \
        -ext SAN=dns:kafka

    # Verify keystore
    # keytool -list -v -keystore kafka.broker1.keystore -storepass confluent

    # Create CSR, sign the key and import back into keystore
    keytool -keystore kafka.$i.keystore -alias $i -certreq -file $i.csr -storepass confluent -keypass confluent -ext SAN=dns:kafka -noprompt

    openssl x509 -req -CA ca-1.crt -CAkey ca-1.key -in $i.csr -out $i-ca1-signed.crt -days 9999 -CAcreateserial -passin pass:confluent

    keytool -keystore kafka.$i.keystore -alias CARoot -import -file ca-1.crt -storepass confluent -keypass confluent -ext SAN=dns:kafka -noprompt

    keytool -keystore kafka.$i.keystore -alias $i -import -file $i-ca1-signed.crt -storepass confluent -keypass confluent -ext SAN=dns:kafka -noprompt

    # Create truststore and import the CA cert.
    keytool -keystore kafka.$i.truststore -alias CARoot -import -file ca-1.crt -storepass confluent -keypass confluent -ext SAN=dns:kafka -noprompt

    echo "confluent" >${i}_sslkey_creds
    echo "confluent" >${i}_keystore_creds
    echo "confluent" >${i}_truststore_creds
done
