#!/usr/bin/env bash
set -xeuo
echo "Generate RSA key"
openssl genrsa -out private.key 2048
echo "Create CRT from config"
openssl req -new -x509 -key private.key -config cert.conf -out private.crt
echo "Bundle private key and certificate for minio"
cat private.key private.crt > private.pem
echo "Convert to DER format"
openssl x509 -outform der -in private.pem -out private.der
[ -e truststore.jks ] && rm truststore.jks
echo "Import cert to truststore.jks"
keytool -import -file private.der -alias minio -keystore truststore.jks -storepass '123456' -noprompt
rm private.der private.pem
