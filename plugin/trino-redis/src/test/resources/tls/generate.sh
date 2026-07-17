#!/usr/bin/env bash
# Regenerates the TLS material in this directory used by TestRedisTlsConnectorSmokeTest.

set -euo pipefail

PASS=changeit
DAYS=36500

openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days $DAYS -subj "/CN=Trino Redis Test CA" -out ca.crt

# server certificate
openssl genrsa -out redis.key 2048
openssl req -new -key redis.key -subj "/CN=localhost" -out redis.csr
openssl x509 -req -in redis.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $DAYS -sha256 \
    -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') -out redis.crt

# client certificate (mTLS)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -subj "/CN=trino-redis-client" -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $DAYS -sha256 \
    -extfile <(printf 'extendedKeyUsage=clientAuth\n') -out client.crt

openssl pkcs12 -export -inkey client.key -in client.crt -certfile ca.crt -name client -out keystore.p12 -passout pass:$PASS
rm -f truststore.p12
keytool -importcert -noprompt -alias ca -file ca.crt -keystore truststore.p12 -storetype PKCS12 -storepass $PASS

rm -f ca.key ca.srl client.key client.crt client.csr redis.csr
