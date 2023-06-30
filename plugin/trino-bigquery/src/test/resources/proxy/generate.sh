#!/usr/bin/env bash
set -xeuo
echo "Generate RSA key"
openssl genrsa -out cert.key 2048
echo "Create CRT from config"
openssl req -new -x509 -key cert.key -config cert.conf -out cert.crt
echo "Bundle private key and certificate for mitmproxy"
cat cert.key cert.crt > cert.pem
echo "Convert to DER format"
openssl x509 -outform der -in cert.pem -out cert.der
[ -e truststore.jks ] && rm truststore.jks
echo "Import cert to trustore.jks"
keytool -import -file cert.der -alias googleapis -keystore truststore.jks -storepass '123456' -noprompt
rm cert.der cert.key
