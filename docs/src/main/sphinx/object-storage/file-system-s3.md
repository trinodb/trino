# S3 file system support

Trino includes a native implementation to access [Amazon
S3](https://aws.amazon.com/s3/) and compatible storage systems with a catalog
using the Delta Lake, Hive, Hudi, or Iceberg connectors.

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
  - Activate the native implementation for S3 storage support, and deactivate
    all [legacy support](file-system-legacy). Defaults to `false`. Must be set
    to `true` for all other properties be used.
* - `s3.endpoint`
  - Required endpoint URL for S3.
* - `s3.region`
  - Required region name for S3.
* - `s3.path-style-access`
  - Use path-style access for all requests to S3
* - `s3.sse.type`
  - Set the type of S3 server-side encryption (SSE) to use. Defaults to `NONE`
    for no encryption. Other valid values are `S3` for encryption by S3 managed
    keys, and `KMS` for encryption with a key from the AWS Key Management
    Service (KMS). Note that S3 automatically uses SSE so `NONE` and `S3` are
    equivalent. S3-compatible systems might behave differently.
* - `s3.sse.kms-key-id`
  - The identifier of a key in KMS to use for SSE.
* - `s3.streaming.part-size`
  - Part size for S3 streaming upload. Values between `5MB` and `256MB` are
    valid. Defaults to `16MB`.
* - `s3.requester-pays`
  - Switch to activate billing transfer cost to the requester. Defaults to
    `false`.
* - `s3.max-connections`
  - Maximum number of connections to S3.
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
  - Set to `true` to enable HTTPS for the proxy server..
:::

## Authentication

Use the following properties to configure the authentication to S3 with access
and secret keys, STS, or an IAM role:

:::{list-table}
:widths: 40, 60
:header-rows: 1

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
