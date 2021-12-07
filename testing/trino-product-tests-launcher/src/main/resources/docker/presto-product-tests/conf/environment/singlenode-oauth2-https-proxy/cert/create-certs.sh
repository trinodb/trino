#!/bin/sh

set -eux

for i in hydra proxy; do
    echo $i

    # Generate private key and certificate
    openssl req -new -x509 -newkey rsa:4096 -sha256 -nodes -keyout $i.key -days 3650 -out $i.crt -config $i.conf
    cat $i.crt $i.key > $i.pem

    # Create truststore and import the cert.
    keytool -keystore truststore.jks -alias $i -import -file $i.crt -storepass changeit -noprompt
    rm $i.crt $i.key
done
