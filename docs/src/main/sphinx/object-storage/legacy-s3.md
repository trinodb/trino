# Legacy S3 support

The {doc}`/connector/hive` can read and write tables that are stored in
[Amazon S3](https://aws.amazon.com/s3/) or S3-compatible systems.
This is accomplished by having a table or database location that
uses an S3 prefix, rather than an HDFS prefix.

Trino uses its own S3 filesystem for the URI prefixes
`s3://`, `s3n://` and  `s3a://`.

:::{warning}
Legacy support is not recommended and will be removed. Use [](file-system-s3).
:::

To use legacy support, the `fs.hadoop.enabled` property must be set to `true` in
your catalog configuration file.

(hive-s3-configuration)=
## S3 configuration properties

:::{list-table}
:widths: 35, 65
:header-rows: 1

* - Property name
  - Description
* - `hive.s3.aws-access-key`
  - Default AWS access key to use.
* - `hive.s3.aws-secret-key`
  - Default AWS secret key to use.
* - `hive.s3.iam-role`
  - IAM role to assume.
* - `hive.s3.external-id`
  - External ID for the IAM role trust policy.
* - `hive.s3.endpoint`
  - The S3 storage endpoint server. This can be used to connect to an
    S3-compatible storage system instead of AWS. When using v4 signatures, it is
    recommended to set this to the AWS region-specific endpoint (e.g.,
    `http[s]://s3.<AWS-region>.amazonaws.com`).
* - `hive.s3.region`
  - Optional property to force the S3 client to connect to the specified region
    only.
* - `hive.s3.storage-class`
  - The S3 storage class to use when writing the data. Currently only `STANDARD`
    and `INTELLIGENT_TIERING` storage classes are supported. Default storage
    class is `STANDARD`
* - `hive.s3.signer-type`
  - Specify a different signer type for S3-compatible storage. Example:
    `S3SignerType` for v2 signer type
* - `hive.s3.signer-class`
  - Specify a different signer class for S3-compatible storage.
* - `hive.s3.path-style-access`
  - Use path-style access for all requests to the S3-compatible storage. This is
    for S3-compatible storage that doesn't support virtual-hosted-style access,
    defaults to `false`.
* - `hive.s3.staging-directory`
  - Local staging directory for data written to S3. This defaults to the Java
    temporary directory specified by the JVM system property `java.io.tmpdir`.
* - `hive.s3.pin-client-to-current-region`
  - Pin S3 requests to the same region as the EC2 instance where Trino is
    running, defaults to `false`.
* - `hive.s3.ssl.enabled`
  - Use HTTPS to communicate with the S3 API, defaults to `true`.
* - `hive.s3.sse.enabled`
  - Use S3 server-side encryption, defaults to `false`.
* - `hive.s3.sse.type`
  - The type of key management for S3 server-side encryption. Use `S3` for S3
    managed or `KMS` for KMS-managed keys, defaults to `S3`.
* - `hive.s3.sse.kms-key-id`
  - The KMS Key ID to use for S3 server-side encryption with KMS-managed keys.
    If not set, the default key is used.
* - `hive.s3.kms-key-id`
  - If set, use S3 client-side encryption and use the AWS KMS to store
    encryption keys and use the value of this property as the KMS Key ID for
    newly created objects.
* - `hive.s3.encryption-materials-provider`
  - If set, use S3 client-side encryption and use the value of this property as
    the fully qualified name of a Java class which implements the AWS SDK's
    `EncryptionMaterialsProvider` interface. If the class also implements
    `Configurable` from the Hadoop API, the Hadoop configuration will be passed
    in after the object has been created.
* - `hive.s3.upload-acl-type`
  - Canned ACL to use while uploading files to S3, defaults to `PRIVATE`. If the
    files are to be uploaded to an S3 bucket owned by a different AWS user, the
    canned ACL has to be set to one of the following: `AUTHENTICATED_READ`,
    `AWS_EXEC_READ`, `BUCKET_OWNER_FULL_CONTROL`, `BUCKET_OWNER_READ`,
    `LOG_DELIVERY_WRITE`, `PUBLIC_READ`, `PUBLIC_READ_WRITE`. Refer to the `AWS
    canned ACL
    <https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-s3-acls.html>`_
    guide to understand each option's definition.
* - `hive.s3.skip-glacier-objects`
  - Ignore Glacier objects rather than failing the query. This skips data that
    may be expected to be part of the table or partition. Defaults to `false`.
* - `hive.s3.streaming.enabled`
  - Use S3 multipart upload API to upload file in streaming way, without staging
    file to be created in the local file system. Defaults to `true`.
* - `hive.s3.streaming.part-size`
  - The part size for S3 streaming upload. Defaults to `16MB`.
* - `hive.s3.proxy.host`
  - Proxy host to use if connecting through a proxy.
* - `hive.s3.proxy.port`
  - Proxy port to use if connecting through a proxy.
* - `hive.s3.proxy.protocol`
  - Proxy protocol. HTTP or HTTPS , defaults to `HTTPS`.
* - `hive.s3.proxy.non-proxy-hosts`
  - Hosts list to access without going through the proxy.
* - `hive.s3.proxy.username`
  - Proxy user name to use if connecting through a proxy.
* - `hive.s3.proxy.password`
  - Proxy password to use if connecting through a proxy.
* - `hive.s3.proxy.preemptive-basic-auth`
  - Whether to attempt to authenticate preemptively against proxy when using
    base authorization, defaults to `false`.
* - `hive.s3.sts.endpoint`
  - Optional override for the sts endpoint given that IAM role based
    authentication via sts is used.
* - `hive.s3.sts.region`
  - Optional override for the sts region given that IAM role based
    authentication via sts is used.
* - `hive.s3.storage-class-filter`
  - Filter based on storage class of S3 object, defaults to `READ_ALL`.
  
:::

(hive-s3-credentials)=
## S3 credentials

If you are running Trino on Amazon EC2, using EMR or another facility,
it is recommended that you use IAM Roles for EC2 to govern access to S3.
To enable this, your EC2 instances need to be assigned an IAM Role which
grants appropriate access to the data stored in the S3 bucket(s) you wish
to use. It is also possible to configure an IAM role with `hive.s3.iam-role`
that is used for accessing any S3 bucket. This is much cleaner than
setting AWS access and secret keys in the `hive.s3.aws-access-key`
and `hive.s3.aws-secret-key` settings, and also allows EC2 to automatically
rotate credentials on a regular basis without any additional work on your part.

If you are running Trino on Amazon EKS, and authenticate using a Kubernetes
service account, you can set the
`trino.s3.use-web-identity-token-credentials-provider` to `true`, so Trino does
not try using different credential providers from the default credential
provider chain. The property must be set in the Hadoop configuration files
referenced by the `hive.config.resources` Hive connector property.

## Custom S3 credentials provider

You can configure a custom S3 credentials provider by setting the configuration
property `trino.s3.credentials-provider` to the fully qualified class name of
a custom AWS credentials provider implementation. The property must be set in
the Hadoop configuration files referenced by the `hive.config.resources` Hive
connector property.

The class must implement the
[AWSCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html)
interface and provide a two-argument constructor that takes a
`java.net.URI` and a Hadoop `org.apache.hadoop.conf.Configuration`
as arguments. A custom credentials provider can be used to provide
temporary credentials from STS (using `STSSessionCredentialsProvider`),
IAM role-based credentials (using `STSAssumeRoleSessionCredentialsProvider`),
or credentials for a specific use case (e.g., bucket/user specific credentials).

(hive-s3-security-mapping)=
## S3 security mapping

Trino supports flexible security mapping for S3, allowing for separate
credentials or IAM roles for specific users or buckets/paths. The IAM role
for a specific query can be selected from a list of allowed roles by providing
it as an *extra credential*.

Each security mapping entry may specify one or more match criteria. If multiple
criteria are specified, all criteria must match. Available match criteria:

- `user`: Regular expression to match against username. Example: `alice|bob`
- `group`: Regular expression to match against any of the groups that the user
  belongs to. Example: `finance|sales`
- `prefix`: S3 URL prefix. It can specify an entire bucket or a path within a
  bucket. The URL must start with `s3://` but will also match `s3a` or `s3n`.
  Example: `s3://bucket-name/abc/xyz/`

The security mapping must provide one or more configuration settings:

- `accessKey` and `secretKey`: AWS access key and secret key. This overrides
  any globally configured credentials, such as access key or instance credentials.
- `iamRole`: IAM role to use if no user provided role is specified as an
  extra credential. This overrides any globally configured IAM role. This role
  is allowed to be specified as an extra credential, although specifying it
  explicitly has no effect, as it would be used anyway.
- `roleSessionName`: Optional role session name to use with `iamRole`. This can only
  be used when `iamRole` is specified. If `roleSessionName` includes the string
  `${USER}`, then the `${USER}` portion of the string will be replaced with the
  current session's username. If `roleSessionName` is not specified, it defaults
  to `trino-session`.
- `allowedIamRoles`: IAM roles that are allowed to be specified as an extra
  credential. This is useful because a particular AWS account may have permissions
  to use many roles, but a specific user should only be allowed to use a subset
  of those roles.
- `kmsKeyId`: ID of KMS-managed key to be used for client-side encryption.
- `allowedKmsKeyIds`: KMS-managed key IDs that are allowed to be specified as an extra
  credential. If list cotains "\*", then any key can be specified via extra credential.

* ``endpoint``: The S3 storage endpoint server. This optional property can be used
to override S3 endpoints on a per-bucket basis.

* ``region``: The region S3 client should connect to. This optional property can be used
to override S3 regions on a per-bucket basis.

The security mapping entries are processed in the order listed in the configuration
JSON. More specific mappings should thus be specified before less specific mappings.
For example, the mapping list might have URL prefix `s3://abc/xyz/` followed by
`s3://abc/` to allow different configuration for a specific path within a bucket
than for other paths within the bucket. You can set default configuration by not
including any match criteria for the last entry in the list.

In addition to the rules above, the default mapping can contain the optional
`useClusterDefault` boolean property with the following behavior:

- `false` - (is set by default) property is ignored.

- `true` - This causes the default cluster role to be used as a fallback option.
  It can not be used with the following configuration properties:

  - `accessKey`
  - `secretKey`
  - `iamRole`
  - `allowedIamRoles`

If no mapping entry matches and no default is configured, the access is denied.

The configuration JSON can either be retrieved from a file or REST-endpoint specified via
`hive.s3.security-mapping.config-file`.

Example JSON configuration:

```json
{
  "mappings": [
    {
      "prefix": "s3://bucket-name/abc/",
      "iamRole": "arn:aws:iam::123456789101:role/test_path"
    },
    {
      "user": "bob|charlie",
      "iamRole": "arn:aws:iam::123456789101:role/test_default",
      "allowedIamRoles": [
        "arn:aws:iam::123456789101:role/test1",
        "arn:aws:iam::123456789101:role/test2",
        "arn:aws:iam::123456789101:role/test3"
      ]
    },
    {
      "prefix": "s3://special-bucket/",
      "accessKey": "AKIAxxxaccess",
      "secretKey": "iXbXxxxsecret"
    }, 
    {
      "prefix": "s3://regional-bucket/",
      "iamRole": "arn:aws:iam::123456789101:role/regional-user",
      "endpoint": "https://bucket.vpce-1a2b3c4d-5e6f.s3.us-east-1.vpce.amazonaws.com",
      "region": "us-east-1"
    },
    {
      "prefix": "s3://encrypted-bucket/",
      "kmsKeyId": "kmsKey_10"
    },
    {
      "user": "test.*",
      "iamRole": "arn:aws:iam::123456789101:role/test_users"
    },
    {
      "group": "finance",
      "iamRole": "arn:aws:iam::123456789101:role/finance_users"
    },
    {
      "iamRole": "arn:aws:iam::123456789101:role/default"
    }
  ]
}
```

| Property name                                         | Description                                                                                                                                                                                                                                                                                        |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hive.s3.security-mapping.config-file`                | The JSON configuration file or REST-endpoint URI containing security mappings.                                                                                                                                                                                                                     |
| `hive.s3.security-mapping.json-pointer`               | A JSON pointer (RFC 6901) to mappings inside the JSON retrieved from the config file or REST-endpont. The whole document ("") by default.                                                                                                                                                          |
| `hive.s3.security-mapping.iam-role-credential-name`   | The name of the *extra credential* used to provide the IAM role.                                                                                                                                                                                                                                   |
| `hive.s3.security-mapping.kms-key-id-credential-name` | The name of the *extra credential* used to provide the KMS-managed key ID.                                                                                                                                                                                                                         |
| `hive.s3.security-mapping.refresh-period`             | How often to refresh the security mapping configuration.                                                                                                                                                                                                                                           |
| `hive.s3.security-mapping.colon-replacement`          | The character or characters to be used in place of the colon (`:`) character when specifying an IAM role name as an extra credential. Any instances of this replacement value in the extra credential value will be converted to a colon. Choose a value that is not used in any of your IAM ARNs. |

(hive-s3-tuning-configuration)=
## Tuning properties

The following tuning properties affect the behavior of the client
used by the Trino S3 filesystem when communicating with S3.
Most of these parameters affect settings on the `ClientConfiguration`
object associated with the `AmazonS3Client`.

| Property name                     | Description                                                                                       | Default                    |
| --------------------------------- | ------------------------------------------------------------------------------------------------- | -------------------------- |
| `hive.s3.max-error-retries`       | Maximum number of error retries, set on the S3 client.                                            | `10`                       |
| `hive.s3.max-client-retries`      | Maximum number of read attempts to retry.                                                         | `5`                        |
| `hive.s3.max-backoff-time`        | Use exponential backoff starting at 1 second up to this maximum value when communicating with S3. | `10 minutes`               |
| `hive.s3.max-retry-time`          | Maximum time to retry communicating with S3.                                                      | `10 minutes`               |
| `hive.s3.connect-timeout`         | TCP connect timeout.                                                                              | `5 seconds`                |
| `hive.s3.connect-ttl`             | TCP connect TTL, which affects connection reusage.                                                | Connections do not expire. |
| `hive.s3.socket-timeout`          | TCP socket read timeout.                                                                          | `5 seconds`                |
| `hive.s3.max-connections`         | Maximum number of simultaneous open connections to S3.                                            | `500`                      |
| `hive.s3.multipart.min-file-size` | Minimum file size before multi-part upload to S3 is used.                                         | `16 MB`                    |
| `hive.s3.multipart.min-part-size` | Minimum multi-part upload part size.                                                              | `5 MB`                     |

(hive-s3-data-encryption)=
## S3 data encryption

Trino supports reading and writing encrypted data in S3 using both
server-side encryption with S3 managed keys and client-side encryption using
either the Amazon KMS or a software plugin to manage AES encryption keys.

With [S3 server-side encryption](http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html),
called *SSE-S3* in the Amazon documentation, the S3 infrastructure takes care of all encryption and decryption
work. One exception is SSL to the client, assuming you have `hive.s3.ssl.enabled` set to `true`.
S3 also manages all the encryption keys for you. To enable this, set `hive.s3.sse.enabled` to `true`.

With [S3 client-side encryption](http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html),
S3 stores encrypted data and the encryption keys are managed outside of the S3 infrastructure. Data is encrypted
and decrypted by Trino instead of in the S3 infrastructure. In this case, encryption keys can be managed
either by using the AWS KMS, or your own key management system. To use the AWS KMS for key management, set
`hive.s3.kms-key-id` to the UUID of a KMS key. Your AWS credentials or EC2 IAM role will need to be
granted permission to use the given key as well.

To use a custom encryption key management system, set `hive.s3.encryption-materials-provider` to the
fully qualified name of a class which implements the
[EncryptionMaterialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/EncryptionMaterialsProvider.html)
interface from the AWS Java SDK. This class has to be accessible to the Hive Connector through the
classpath and must be able to communicate with your custom key management system. If this class also implements
the `org.apache.hadoop.conf.Configurable` interface from the Hadoop Java API, then the Hadoop configuration
is passed in after the object instance is created, and before it is asked to provision or retrieve any
encryption keys.

## Migration to S3 file system

Trino includes a [native implementation to access Amazon
S3](/object-storage/file-system-s3) with a catalog using the Delta Lake, Hive,
Hudi, or Iceberg connectors. Upgrading existing deployments to the new native
implementation is recommended. Legacy support will be deprecated and removed.

To migrate a catalog to use the native file system implementation for S3, make
the following edits to your catalog configuration:

1. Add the `fs.native-s3.enabled=true` catalog configuration property.
2. Refer to the following table to rename your existing legacy catalog
   configuration properties to the corresponding native configuration
   properties. Supported configuration values are identical unless otherwise
   noted.

  :::{list-table}
  :widths: 35, 35, 65
  :header-rows: 1
   * - Legacy property
     - Native property
     - Notes
   * - `hive.s3.aws-access-key`
     - `s3.aws-access-key`
     -
   * - `hive.s3.aws-secret-key`
     - `s3.aws-secret-key`
     -
   * - `hive.s3.iam-role`
     - `s3.iam-role`
     - Also see `s3.role-session-name` in [](/object-storage/file-system-s3)
       for more role configuration options.
   * - `hive.s3.external-id`
     - `s3.external-id`
     -
   * - `hive.s3.endpoint`
     - `s3.endpoint`
     - Add the `https://` prefix to make the value a correct URL.
   * - `hive.s3.region`
     - `s3.region`
     -
   * - `hive.s3.sse.enabled`
     - None
     - `s3.sse.type` set to the default value of `NONE` is equivalent to
       `hive.s3.sse.enabled=false`.
   * - `hive.s3.sse.type`
     - `s3.sse.type`
     -
   * - `hive.s3.sse.kms-key-id`
     - `s3.sse.kms-key-id`
     -
   * - `hive.s3.upload-acl-type`
     - `s3.canned-acl`
     - See [](/object-storage/file-system-s3) for supported values.
   * - `hive.s3.streaming.part-size`
     - `s3.streaming.part-size`
     -
   * - `hive.s3.proxy.host`, `hive.s3.proxy.port`
     - `s3.http-proxy`
     - Specify the host and port in one URL, for example `localhost:8888`.
   * - `hive.s3.proxy.protocol`
     - `s3.http-proxy.secure`
     - Set to `TRUE` to enable HTTPS.
   * - `hive.s3.proxy.non-proxy-hosts`
     - `s3.http-proxy.non-proxy-hosts`
     -
   * - `hive.s3.proxy.username`
     - `s3.http-proxy.username`
     -
   * - `hive.s3.proxy.password`
     - `s3.http-proxy.password`
     -
   * - `hive.s3.proxy.preemptive-basic-auth`
     - `s3.http-proxy.preemptive-basic-auth`
     -
   * - `hive.s3.sts.endpoint`
     - `s3.sts.endpoint`
     -
   * - `hive.s3.sts.region`
     - `s3.sts.region`
     -
   * - `hive.s3.max-error-retries`
     - `s3.max-error-retries`
     - Also see `s3.retry-mode` in [](/object-storage/file-system-s3) for more
       retry behavior configuration options.
   * - `hive.s3.connect-timeout`
     - `s3.connect-timeout`
     -
   * - `hive.s3.connect-ttl`
     - `s3.connection-ttl`
     - Also see `s3.connection-max-idle-time` in
       [](/object-storage/file-system-s3) for more connection keep-alive
       options.
   * - `hive.s3.socket-timeout`
     - `s3.socket-read-timeout`
     - Also see `s3.tcp-keep-alive` in [](/object-storage/file-system-s3) for
       more socket connection keep-alive options.
   * - `hive.s3.max-connections`
     - `s3.max-connections`
     -
   * - `hive.s3.path-style-access`
     - `s3.path-style-access`
     -
  :::

1. Remove the following legacy configuration properties if they exist in your
   catalog configuration:

      * `hive.s3.storage-class`
      * `hive.s3.signer-type`
      * `hive.s3.signer-class`
      * `hive.s3.staging-directory`
      * `hive.s3.pin-client-to-current-region`
      * `hive.s3.ssl.enabled`
      * `hive.s3.sse.enabled`
      * `hive.s3.kms-key-id`
      * `hive.s3.encryption-materials-provider`
      * `hive.s3.streaming.enabled`
      * `hive.s3.max-client-retries`
      * `hive.s3.max-backoff-time`
      * `hive.s3.max-retry-time`
      * `hive.s3.multipart.min-file-size`
      * `hive.s3.multipart.min-part-size`
      * `hive.s3-file-system-type`
      * `hive.s3.user-agent-prefix`

For more information, see the [](/object-storage/file-system-s3).
