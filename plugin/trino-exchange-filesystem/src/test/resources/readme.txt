Generate certs:

1. Run KES server
docker run -d --name kes-server   minio/kes server --dev

2. Enter kes server
docker exec -it  kes-server   /bin/bash

3. Generate key and cert for KES (10y expiry time)
./kes identity new --ip "127.0.0.1" localhost  --dns kes --key=kms.key --cert=kms.crt --expiry 87660h

You will get output as below:

Your API key:

   kes:v1:ADmUvSFjVFNtq2yEdSmS1qYMhHV5g9GcKa62QXMd3PuE

This is the only time it is shown. Keep it secret and secure!

Your Identity:

   2f90ad9d3d3d7a90e9f8eeb9175913625148b5a34e34467dd9cd24d1b30516e0

The identity is not a secret. It can be shared. Any peer
needs this identity in order to verify your API key.

The generated TLS private key is stored at: kms.key
The generated TLS certificate is stored at: kms.crt

The identity can be computed again via:

    kes identity of kes:v1:ADmUvSFjVFNtq2yEdSmS1qYMhHV5g9GcKa62QXMd3PuE
    kes identity of kms.crt

4. Copy identity value into a file ../kms/config.yml as a value to property admin.identity
i.e.
<config.yml>  excerpt

admin:
  identity: 2f90ad9d3d3d7a90e9f8eeb9175913625148b5a34e34467dd9cd24d1b30516e0


5. Generate key and cert for MinIO (10y expiry time)
./kes identity new --key=kms_client.key --cert=kms_client.crt minio --expiry 87660h

You will get output as below:

Your API key:

   kes:v1:ABNO9w2+ml5m3Co8odj2ztpYydMSBHK8DkvKe/nJ3tYW

This is the only time it is shown. Keep it secret and secure!

Your Identity:

   f8611172f05decf753a653622d215024aacd40d518aee79b1cc6e15e621235d3

The identity is not a secret. It can be shared. Any peer
needs this identity in order to verify your API key.

The generated TLS private key is stored at: kms_client.key
The generated TLS certificate is stored at: kms_client.crt

The identity can be computed again via:

    kes identity of kes:v1:ABNO9w2+ml5m3Co8odj2ztpYydMSBHK8DkvKe/nJ3tYW
    kes identity of kms_client.crt



6. Copy identity value into file ../kms/config.yml as a value for property policy.minio.identities
i.e.
<config.yml>  excerpt

policy:
  minio:
    allow:
        ..
    identities:
      - f8611172f05decf753a653622d215024aacd40d518aee79b1cc6e15e621235d3


7. Copy keys and certs from KES server to local dirs
cd kes
docker cp kes-server:/kms.crt .
docker cp kes-server:/kms.key .
cd ../minio
docker cp kes-server:/kms_client.crt .
docker cp kes-server:/kms_client.key .
