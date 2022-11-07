#!/bin/sh

set -eux

cp "$(dirname "$0")/../../../../common/hydra-identity-provider/cert/truststore.jks" truststore.jks

# Generate private key and certificate
openssl req -new -x509 -newkey rsa:4096 -sha256 -nodes -keyout proxy.key -days 36500 -out proxy.crt -config proxy.conf
cat proxy.crt proxy.key > proxy.pem

# Create truststore and import the cert.
keytool -keystore truststore.jks -alias proxy -import -file proxy.crt -storepass 123456 -noprompt
rm proxy.crt proxy.key
