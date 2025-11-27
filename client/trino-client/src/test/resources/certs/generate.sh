#!/bin/sh

set -euo pipefail

openssl req -new -x509 -newkey rsa:4096 -sha256 -nodes -keyout localhost.key -days 35600 -out localhost.crt -config localhost.conf
openssl pkcs12 -export -in localhost.crt -inkey localhost.key -name localhost -passout pass:Pass1234 > localhost.p12
keytool -importkeystore -srckeystore localhost.p12 -srcstorepass Pass1234 -destkeystore certs.jks -srcstoretype pkcs12 -alias localhost -storepass Pass1234
rm localhost.key localhost.p12

openssl ecparam -out differenthost.key -name secp256r1 -genkey
openssl req -new -x509 -key differenthost.key -out differenthost.crt -days 36500 --config differenthost.conf
openssl pkcs12 -export -in differenthost.crt -inkey differenthost.key -name differenthost -passout pass:Pass1234 > differenthost.p12
keytool -importkeystore -srckeystore differenthost.p12 -srcstorepass Pass1234 -destkeystore certs.jks -srcstoretype pkcs12 -alias differenthost -storepass Pass1234
rm differenthost.key differenthost.p12

cat localhost.crt differenthost.crt > certs.crt
