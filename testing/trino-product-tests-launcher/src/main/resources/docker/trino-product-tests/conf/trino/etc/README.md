### Listing entries in the keystore

```bash
keytool -list -keystore docker.cluster.jks
```

### Removing an entry from the keystore

```bash
keytool -delete -alias <alias> -keystore docker.cluster.jks
```

### Generate a new keypair and add it to the keystore

```bash
keytool \
    -alias <alias> \
    -keystore docker.cluster.jks \
    -storepass 123456 \
    -deststoretype pkcs12 \
    -genkeypair \
    -keyalg RSA \
    -validity 36500 \
    -dname "CN=<alias>.docker.cluster, OU=, O=, L=, ST=, C=" \
    -ext "SAN=DNS:<alias>.docker.cluster"
```
