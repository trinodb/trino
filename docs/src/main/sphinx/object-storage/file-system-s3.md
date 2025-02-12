# S3 file system support

Trino includes a native implementation to access [Amazon
S3](https://aws.amazon.com/s3/) and compatible storage systems with a catalog
using the Delta Lake, Hive, Hudi, or Iceberg connectors. While Trino is designed
to support S3-compatible storage systems, only AWS S3 and MinIO are tested for
compatibility. For other storage systems, perform your own testing and consult
your vendor for more information.

Enable the native implementation with `fs.native-s3.enabled=true` in your
catalog properties file.

## General configuration

Use the following properties to configure general aspects of S3 file system
support:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `fs.native-s3.enabled`
  - Activate the native implementation for S3 storage support. Defaults to
    `false`. Set to `true` to use S3 and enable all other properties.
* - `s3.endpoint`
  - Required endpoint URL for S3.
* - `s3.region`
  - Required region name for S3.
* - `s3.path-style-access`
  - Use path-style access for all requests to S3
* - `s3.exclusive-create`
  - Whether conditional write is supported by the S3-compatible storage. Defaults to `true`.
* - `s3.canned-acl`
  - [Canned ACL](https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl)
    to use when uploading files to S3. Defaults to `NONE`, which has the same
    effect as `PRIVATE`. If the files are to be uploaded to an S3 bucket owned
    by a different AWS user, the canned ACL may be set to one of the following:
    `PRIVATE`, `PUBLIC_READ`, `PUBLIC_READ_WRITE`, `AUTHENTICATED_READ`,
    `BUCKET_OWNER_READ`, or `BUCKET_OWNER_FULL_CONTROL`.
* - `s3.sse.type`
  - Set the type of S3 server-side encryption (SSE) to use. Defaults to `NONE`
    for no encryption. Other valid values are `S3` for encryption by S3 managed
    keys, `KMS` for encryption with a key from the AWS Key Management
    Service (KMS), and `CUSTOMER` for encryption with a customer-provided key
    from `s3.sse.customer-key`. Note that S3 automatically uses SSE so `NONE` 
    and `S3` are equivalent. S3-compatible systems might behave differently.
* - `s3.sse.kms-key-id`
  - The identifier of a key in KMS to use for SSE.
* - `s3.sse.customer-key`
  - The 256-bit, base64-encoded AES-256 encryption key to encrypt or decrypt
    data from S3 when using the SSE-C mode for SSE with `s3.sse.type` set to
    `CUSTOMER`. 
* - `s3.streaming.part-size`
  - Part size for S3 streaming upload. Values between `5MB` and `256MB` are
    valid. Defaults to `16MB`.
* - `s3.requester-pays`
  - Switch to activate billing transfer cost to the requester. Defaults to
    `false`.
* - `s3.max-connections`
  - Maximum number of connections to S3.  Defaults to `500`.
* - `s3.connection-ttl`
  - Maximum time [duration](prop-type-duration) allowed to reuse connections in
    the connection pool before being replaced.
* - `s3.connection-max-idle-time`
  - Maximum time [duration](prop-type-duration) allowed for connections to
    remain idle in the connection pool before being closed.
* - `s3.socket-connect-timeout`
  - Maximum time [duration](prop-type-duration) allowed for socket connection
    requests to complete before timing out.
* - `s3.socket-read-timeout`
  - Maximum time [duration](prop-type-duration) for socket read operations
    before timing out.
* - `s3.tcp-keep-alive`
  - Enable TCP keep alive on created connections. Defaults to `false`.
* - `s3.http-proxy`
  - URL of a HTTP proxy server to use for connecting to S3.
* - `s3.http-proxy.secure`
  - Set to `true` to enable HTTPS for the proxy server.
* - `s3.http-proxy.username`
  - Proxy username to use if connecting through a proxy server.
* - `s3.http-proxy.password`
  - Proxy password to use if connecting through a proxy server.
* - `s3.http-proxy.non-proxy-hosts`
  - Hosts list to access without going through the proxy server.
* - `s3.http-proxy.preemptive-basic-auth`
  - Whether to attempt to authenticate preemptively against proxy server
    when using base authorization, defaults to `false`.
* - `s3.retry-mode`
  - Specifies how the AWS SDK attempts retries. Default value is `LEGACY`.
    Other allowed values are `STANDARD` and `ADAPTIVE`. The `STANDARD` mode
    includes a standard set of errors that are retried. `ADAPTIVE` mode
    includes the functionality of `STANDARD` mode with automatic client-side 
    throttling.
* - `s3.max-error-retries`
  - Specifies maximum number of retries the client will make on errors.
    Defaults to `10`.
* - `s3.use-web-identity-token-credentials-provider`
  - Set to `true` to only use the web identity token credentials provider,
    instead of the default providers chain. This can be useful when running
    Trino on Amazon EKS and using [IAM roles for service accounts
    (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
    Defaults to `false`.
* - `s3.application-id`
  - Specify the application identifier appended to the `User-Agent` header 
    for all requests sent to S3. Defaults to `Trino`.
:::

## Authentication

Use the following properties to configure the authentication to S3 with access
and secret keys, STS, or an IAM role:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `s3.aws-access-key`
  - AWS access key to use for authentication.
* - `s3.aws-secret-key`
  - AWS secret key to use for authentication.
* - `s3.sts.endpoint`
  - The endpoint URL of the AWS Security Token Service to use for authenticating
    to S3.
* - `s3.sts.region`
  - AWS region of the STS service.
* - `s3.iam-role`
  - ARN of an IAM role to assume when connecting to S3.
* - `s3.role-session-name`
  - Role session name to use when connecting to S3. Defaults to
    `trino-filesystem`.
* - `s3.external-id`
  - External ID for the IAM role trust policy when connecting to S3.
:::

## Security mapping

Trino supports flexible security mapping for S3, allowing for separate
credentials or IAM roles for specific users or S3 locations. The IAM role
for a specific query can be selected from a list of allowed roles by providing
it as an *extra credential*.

Each security mapping entry may specify one or more match criteria.
If multiple criteria are specified, all criteria must match.
The following match criteria are available:

- `user`: Regular expression to match against username. Example: `alice|bob`
- `group`: Regular expression to match against any of the groups that the user
  belongs to. Example: `finance|sales`
- `prefix`: S3 URL prefix. You can specify an entire bucket or a path within a
  bucket. The URL must start with `s3://` but also matches for `s3a` or `s3n`.
  Example: `s3://bucket-name/abc/xyz/`

The security mapping must provide one or more configuration settings:

- `accessKey` and `secretKey`: AWS access key and secret key. This overrides
  any globally configured credentials, such as access key or instance credentials.
- `iamRole`: IAM role to use if no user provided role is specified as an
  extra credential. This overrides any globally configured IAM role. This role
  is allowed to be specified as an extra credential, although specifying it
  explicitly has no effect.
- `roleSessionName`: Optional role session name to use with `iamRole`. This can only
  be used when `iamRole` is specified. If `roleSessionName` includes the string
  `${USER}`, then the `${USER}` portion of the string is replaced with the
  current session's username. If `roleSessionName` is not specified, it defaults
  to `trino-session`.
- `allowedIamRoles`: IAM roles that are allowed to be specified as an extra
  credential. This is useful because a particular AWS account may have permissions
  to use many roles, but a specific user should only be allowed to use a subset
  of those roles.
- `kmsKeyId`: ID of KMS-managed key to be used for client-side encryption.
- `allowedKmsKeyIds`: KMS-managed key IDs that are allowed to be specified as an extra
  credential. If list cotains `*`, then any key can be specified via extra credential.
- `sseCustomerKey`: The customer provided key (SSE-C) for server-side encryption.
- `allowedSseCustomerKey`: The SSE-C keys that are allowed to be specified as an extra
  credential. If list cotains `*`, then any key can be specified via extra credential.
- `endpoint`: The S3 storage endpoint server. This optional property can be used
  to override S3 endpoints on a per-bucket basis.
- `region`: The S3 region to connect to. This optional property can be used
  to override S3 regions on a per-bucket basis.

The security mapping entries are processed in the order listed in the JSON configuration.
Therefore, specific mappings must be specified before less specific mappings.
For example, the mapping list might have URL prefix `s3://abc/xyz/` followed by
`s3://abc/` to allow different configuration for a specific path within a bucket
than for other paths within the bucket. You can specify the default configuration
by not including any match criteria for the last entry in the list.

In addition to the preceding rules, the default mapping can contain the optional
`useClusterDefault` boolean property set to `true` to use the default S3 configuration.
It cannot be used with any other configuration settings.

If no mapping entry matches and no default is configured, access is denied.

The configuration JSON is read from a file via `s3.security-mapping.config-file`
or from an HTTP endpoint via `s3.security-mapping.config-uri`.

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

:::{list-table} Security mapping properties
:header-rows: 1

* - Property name
  - Description
* - `s3.security-mapping.enabled`
  - Activate the security mapping feature. Defaults to `false`.
    Must be set to `true` for all other properties be used.
* - `s3.security-mapping.config-file`
  - Path to the JSON configuration file containing security mappings.
* - `s3.security-mapping.config-uri`
  - HTTP endpoint URI containing security mappings.
* - `s3.security-mapping.json-pointer`
  - A JSON pointer (RFC 6901) to mappings inside the JSON retrieved from the
    configuration file or HTTP endpoint. The default is the root of the document.
* - `s3.security-mapping.iam-role-credential-name`
  - The name of the *extra credential* used to provide the IAM role.
* - `s3.security-mapping.kms-key-id-credential-name`
  - The name of the *extra credential* used to provide the KMS-managed key ID.
* - `s3.security-mapping.sse-customer-key-credential-name`
  - The name of the *extra credential* used to provide the server-side encryption with customer-provided keys (SSE-C).
* - `s3.security-mapping.refresh-period`
  - How often to refresh the security mapping configuration, specified as a
    {ref}`prop-type-duration`. By default, the configuration is not refreshed.
* - `s3.security-mapping.colon-replacement`
  - The character or characters to be used instead of a colon character
    when specifying an IAM role name as an extra credential.
    Any instances of this replacement value in the extra credential value
    are converted to a colon.
    Choose a value not used in any of your IAM ARNs.
:::


(fs-legacy-s3-migration)=
## Migration from legacy S3 file system

Trino includes legacy Amazon S3 support to use with a catalog using the Delta
Lake, Hive, Hudi, or Iceberg connectors. Upgrading existing deployments to the
current native implementation is recommended. Legacy support is deprecated and
will be removed.

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
     - Also see `s3.role-session-name` in preceding sections
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
     - See preceding sections for supported values.
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
     - Also see `s3.retry-mode` in preceding sections for more retry behavior
       configuration options.
   * - `hive.s3.connect-timeout`
     - `s3.connect-timeout`
     -
   * - `hive.s3.connect-ttl`
     - `s3.connection-ttl`
     - Also see `s3.connection-max-idle-time` in preceding section for more
       connection keep-alive options.
   * - `hive.s3.socket-timeout`
     - `s3.socket-read-timeout`
     - Also see `s3.tcp-keep-alive` in preceding sections for more socket
       connection keep-alive options.
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
