# Test certificates

`ca.crt`, `server.crt`, `server.key` and `truststore.jks` set up TLS for the test
Elasticsearch server. `plugin/trino-opensearch/src/test/resources` holds copies of
them under different names. The certificates are valid for 5 years.

`truststore.jks` uses the password `123456`. The OpenSearch truststore also holds
the server certificate. OpenSearch needs the key in PKCS8 format.

Regenerate both sets from the repository root with:

```bash
docker run --rm --user "$(id -u):0" --volume "$PWD/plugin:/out" \
    --entrypoint bash docker.elastic.co/elasticsearch/elasticsearch:8.11.3 -c '
set -euo pipefail
certutil=/usr/share/elasticsearch/bin/elasticsearch-certutil
keytool=/usr/share/elasticsearch/jdk/bin/keytool
cd /tmp
$certutil ca --silent --pem --days 1825 --out /tmp/ca.zip
unzip -q ca.zip
$certutil cert --silent --pem --days 1825 --ca-cert /tmp/ca/ca.crt --ca-key /tmp/ca/ca.key \
    --name elasticsearch-server --dns localhost,elasticsearch-server --ip 127.0.0.1 \
    --out /tmp/server.zip
unzip -q server.zip
crt=elasticsearch-server/elasticsearch-server.crt
key=elasticsearch-server/elasticsearch-server.key
$keytool -importcert -noprompt -alias elasticsearch -file ca/ca.crt \
    -keystore es-truststore.jks -storetype PKCS12 -storepass 123456
$keytool -importcert -noprompt -alias esnode -file $crt \
    -keystore os-truststore.jks -storetype JKS -storepass 123456
$keytool -importcert -noprompt -alias root-ca -file ca/ca.crt \
    -keystore os-truststore.jks -storetype JKS -storepass 123456
openssl pkcs8 -topk8 -nocrypt -in $key -out key-pkcs8.pem

es=/out/trino-elasticsearch/src/test/resources
cp ca/ca.crt $es/ca.crt
cp $crt $es/server.crt
cp $key $es/server.key
cp es-truststore.jks $es/truststore.jks

os=/out/trino-opensearch/src/test/resources
cp ca/ca.crt $os/root-ca.pem
cp $crt $os/esnode.pem
cp key-pkcs8.pem $os/esnode-key.pem
cp key-pkcs8.pem $os/serverkey.pem
cp os-truststore.jks $os/truststore.jks
'
```
