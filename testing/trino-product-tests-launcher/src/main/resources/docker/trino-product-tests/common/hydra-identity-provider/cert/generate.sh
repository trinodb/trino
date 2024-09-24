#!/bin/sh

set -eux

rm -f truststore.jks

for name in "hydra" "trino"
do
    openssl req -new -x509 -newkey rsa:4096 -sha256 -nodes -keyout "${name}.key" -days 35600 -out "${name}.crt" -config "${name}.conf"
    cat "${name}.crt" "${name}.key" > "${name}.pem"
    keytool -import -noprompt -alias "${name}" -keystore truststore.jks -storepass 123456 -file "${name}.crt"
    rm "${name}.crt" "${name}.key"
done
