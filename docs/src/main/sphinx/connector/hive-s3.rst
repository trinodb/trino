=============================
Hive connector with Amazon S3
=============================

The :doc:`hive` can read and write tables that are stored in
`Amazon S3  <https://aws.amazon.com/s3/>`_ or S3-compatible systems.
This is accomplished by having a table or database location that
uses an S3 prefix, rather than an HDFS prefix.

Trino uses its own S3 filesystem for the URI prefixes
``s3://``, ``s3n://`` and  ``s3a://``.

S3 configuration properties
---------------------------

============================================ =================================================================
Property Name                                Description
============================================ =================================================================
``hive.s3.aws-access-key``                   Default AWS access key to use.

``hive.s3.aws-secret-key``                   Default AWS secret key to use.

``hive.s3.iam-role``                         IAM role to assume.

``hive.s3.external-id``                      External ID for the IAM role trust policy.

``hive.s3.endpoint``                         The S3 storage endpoint server. This can be used to
                                             connect to an S3-compatible storage system instead
                                             of AWS. When using v4 signatures, it is recommended to
                                             set this to the AWS region-specific endpoint
                                             (e.g., ``http[s]://s3.<AWS-region>.amazonaws.com``).

``hive.s3.storage-class``                    The S3 storage class to use when writing the data. Currently only
                                             ``STANDARD`` and ``INTELLIGENT_TIERING`` storage classes are supported.
                                             Default storage class is ``STANDARD``

``hive.s3.signer-type``                      Specify a different signer type for S3-compatible storage.
                                             Example: ``S3SignerType`` for v2 signer type

``hive.s3.signer-class``                     Specify a different signer class for S3-compatible storage.

``hive.s3.path-style-access``                Use path-style access for all requests to the S3-compatible storage.
                                             This is for S3-compatible storage that doesn't support virtual-hosted-style access,
                                             defaults to ``false``.

``hive.s3.staging-directory``                Local staging directory for data written to S3.
                                             This defaults to the Java temporary directory specified
                                             by the JVM system property ``java.io.tmpdir``.

``hive.s3.pin-client-to-current-region``     Pin S3 requests to the same region as the EC2
                                             instance where Trino is running,
                                             defaults to ``false``.

``hive.s3.ssl.enabled``                      Use HTTPS to communicate with the S3 API, defaults to ``true``.

``hive.s3.sse.enabled``                      Use S3 server-side encryption, defaults to ``false``.

``hive.s3.sse.type``                         The type of key management for S3 server-side encryption.
                                             Use ``S3`` for S3 managed or ``KMS`` for KMS-managed keys,
                                             defaults to ``S3``.

``hive.s3.sse.kms-key-id``                   The KMS Key ID to use for S3 server-side encryption with
                                             KMS-managed keys. If not set, the default key is used.

``hive.s3.kms-key-id``                       If set, use S3 client-side encryption and use the AWS
                                             KMS to store encryption keys and use the value of
                                             this property as the KMS Key ID for newly created
                                             objects.

``hive.s3.encryption-materials-provider``    If set, use S3 client-side encryption and use the
                                             value of this property as the fully qualified name of
                                             a Java class which implements the AWS SDK's
                                             ``EncryptionMaterialsProvider`` interface.   If the
                                             class also implements ``Configurable`` from the Hadoop
                                             API, the Hadoop configuration will be passed in after
                                             the object has been created.

``hive.s3.upload-acl-type``                  Canned ACL to use while uploading files to S3, defaults
                                             to ``PRIVATE``. If the files are to be uploaded to an S3
                                             bucket owned by a different AWS user, the canned ACL has to be
                                             set to one of the following: ``AUTHENTICATED_READ``,
                                             ``AWS_EXEC_READ``, ``BUCKET_OWNER_FULL_CONTROL``, ``BUCKET_OWNER_READ``,
                                             ``LOG_DELIVERY_WRITE``, ``PUBLIC_READ``, ``PUBLIC_READ_WRITE``.
                                             Refer to the `AWS canned ACL <https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-s3-acls.html>`_
                                             guide to understand each option's definition.

``hive.s3.skip-glacier-objects``             Ignore Glacier objects rather than failing the query. This
                                             skips data that may be expected to be part of the table
                                             or partition. Defaults to ``false``.

``hive.s3.streaming.enabled``                Use S3 multipart upload API to upload file in streaming way,
                                             without staging file to be created in the local file system.

``hive.s3.streaming.part-size``              The part size for S3 streaming upload. Defaults to ``16MB``.

``hive.s3.proxy.host``                       Proxy host to use if connecting through a proxy

``hive.s3.proxy.port``                       Proxy port to use if connecting through a proxy

``hive.s3.proxy.protocol``                   Proxy protocol. HTTP or HTTPS , defaults to ``HTTPS``.

``hive.s3.proxy.non-proxy-hosts``            Hosts list to access without going through the proxy.

``hive.s3.proxy.username``                   Proxy user name to use if connecting through a proxy

``hive.s3.proxy.password``                   Proxy password name to use if connecting through a proxy

``hive.s3.proxy.preemptive-basic-auth``      Whether to attempt to authenticate preemptively against proxy
                                             when using base authorization, defaults to ``false``.

============================================ =================================================================

.. _hive-s3-credentials:

S3 credentials
--------------

If you are running Trino on Amazon EC2, using EMR or another facility,
it is recommended that you use IAM Roles for EC2 to govern access to S3.
To enable this, your EC2 instances need to be assigned an IAM Role which
grants appropriate access to the data stored in the S3 bucket(s) you wish
to use. It is also possible to configure an IAM role with ``hive.s3.iam-role``
that is used for accessing any S3 bucket. This is much cleaner than
setting AWS access and secret keys in the ``hive.s3.aws-access-key``
and ``hive.s3.aws-secret-key`` settings, and also allows EC2 to automatically
rotate credentials on a regular basis without any additional work on your part.

Custom S3 credentials provider
------------------------------

You can configure a custom S3 credentials provider by setting the configuration
property ``trino.s3.credentials-provider`` to the fully qualified class name of
a custom AWS credentials provider implementation. The property must be set in
the Hadoop configuration files referenced by the ``hive.config.resources`` Hive
connector property.

The class must implement the
`AWSCredentialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html>`_
interface and provide a two-argument constructor that takes a
``java.net.URI`` and a Hadoop ``org.apache.hadoop.conf.Configuration``
as arguments. A custom credentials provider can be used to provide
temporary credentials from STS (using ``STSSessionCredentialsProvider``),
IAM role-based credentials (using ``STSAssumeRoleSessionCredentialsProvider``),
or credentials for a specific use case (e.g., bucket/user specific credentials).


.. _hive-s3-security-mapping:

S3 security mapping
-------------------

Trino supports flexible security mapping for S3, allowing for separate
credentials or IAM roles for specific users or buckets/paths. The IAM role
for a specific query can be selected from a list of allowed roles by providing
it as an *extra credential*.

Each security mapping entry may specify one or more match criteria. If multiple
criteria are specified, all criteria must match. Available match criteria:

* ``user``: Regular expression to match against username. Example: ``alice|bob``

* ``group``: Regular expression to match against any of the groups that the user
  belongs to. Example: ``finance|sales``

* ``prefix``: S3 URL prefix. It can specify an entire bucket or a path within a
  bucket. The URL must start with ``s3://`` but will also match ``s3a`` or ``s3n``.
  Example: ``s3://bucket-name/abc/xyz/``

The security mapping must provide one or more configuration settings:

* ``accessKey`` and ``secretKey``: AWS access key and secret key. This overrides
  any globally configured credentials, such as access key or instance credentials.

* ``iamRole``: IAM role to use if no user provided role is specified as an
  extra credential. This overrides any globally configured IAM role. This role
  is allowed to be specified as an extra credential, although specifying it
  explicitly has no effect, as it would be used anyway.

* ``roleSessionName``: Optional role session name to use with ``iamRole``. This can only
  be used when ``iamRole`` is specified. If ``roleSessionName`` includes the string
  ``${USER}``, then the ``${USER}`` portion of the string will be replaced with the
  current session's username. If ``roleSessionName`` is not specified, it defaults
  to ``trino-session``.

* ``allowedIamRoles``: IAM roles that are allowed to be specified as an extra
  credential. This is useful because a particular AWS account may have permissions
  to use many roles, but a specific user should only be allowed to use a subset
  of those roles.

* ``kmsKeyId``: ID of KMS-managed key to be used for client-side encryption.

* ``allowedKmsKeyIds``: KMS-managed key IDs that are allowed to be specified as an extra
  credential. If list cotains "*", then any key can be specified via extra credential.

The security mapping entries are processed in the order listed in the configuration
JSON. More specific mappings should thus be specified before less specific mappings.
For example, the mapping list might have URL prefix ``s3://abc/xyz/`` followed by
``s3://abc/`` to allow different configuration for a specific path within a bucket
than for other paths within the bucket. You can set default configuration by not
including any match criteria for the last entry in the list.

In addition to the rules above, the default mapping can contain the optional
``useClusterDefault`` boolean property with the following behavior:

- ``false`` - (is set by default) property is ignored.
- ``true`` - This causes the default cluster role to be used as a fallback option.
  It can not be used with the following configuration properties:

  - ``accessKey``
  - ``secretKey``
  - ``iamRole``
  - ``allowedIamRoles``

If no mapping entry matches and no default is configured, the access is denied.

The configuration JSON can either be retrieved from a file or REST-endpoint specified via
``hive.s3.security-mapping.config-file``.

Example JSON configuration:

.. code-block:: json

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
          "prefix": "s3://encrypted-bucket/",
          "kmsKeyId": "kmsKey_10",
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

======================================================= =================================================================
Property Name                                           Description
======================================================= =================================================================
``hive.s3.security-mapping.config-file``                The JSON configuration file or REST-endpoint URI containing
                                                        security mappings.
``hive.s3.security-mapping.json-pointer``               A JSON pointer (RFC 6901) to mappings inside the JSON retrieved from
                                                        the config file or REST-endpont. The whole document ("") by default.

``hive.s3.security-mapping.iam-role-credential-name``   The name of the *extra credential* used to provide the IAM role.

``hive.s3.security-mapping.kms-key-id-credential-name`` The name of the *extra credential* used to provide the
                                                        KMS-managed key ID.

``hive.s3.security-mapping.refresh-period``             How often to refresh the security mapping configuration.

``hive.s3.security-mapping.colon-replacement``          The character or characters to be used in place of the colon
                                                        (``:``) character when specifying an IAM role name as an
                                                        extra credential. Any instances of this replacement value in the
                                                        extra credential value will be converted to a colon. Choose a
                                                        value that is not used in any of your IAM ARNs.
======================================================= =================================================================

Tuning properties
-----------------

The following tuning properties affect the behavior of the client
used by the Trino S3 filesystem when communicating with S3.
Most of these parameters affect settings on the ``ClientConfiguration``
object associated with the ``AmazonS3Client``.

===================================== =========================================================== ===============
Property Name                         Description                                                 Default
===================================== =========================================================== ===============
``hive.s3.max-error-retries``         Maximum number of error retries, set on the S3 client.      ``10``

``hive.s3.max-client-retries``        Maximum number of read attempts to retry.                   ``5``

``hive.s3.max-backoff-time``          Use exponential backoff starting at 1 second up to          ``10 minutes``
                                      this maximum value when communicating with S3.

``hive.s3.max-retry-time``            Maximum time to retry communicating with S3.                ``10 minutes``

``hive.s3.connect-timeout``           TCP connect timeout.                                        ``5 seconds``

``hive.s3.socket-timeout``            TCP socket read timeout.                                    ``5 seconds``

``hive.s3.max-connections``           Maximum number of simultaneous open connections to S3.      ``500``

``hive.s3.multipart.min-file-size``   Minimum file size before multi-part upload to S3 is used.   ``16 MB``

``hive.s3.multipart.min-part-size``   Minimum multi-part upload part size.                        ``5 MB``
===================================== =========================================================== ===============

S3 data encryption
------------------


Trino supports reading and writing encrypted data in S3 using both
server-side encryption with S3 managed keys and client-side encryption using
either the Amazon KMS or a software plugin to manage AES encryption keys.

With `S3 server-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html>`_,
called *SSE-S3* in the Amazon documentation, the S3 infrastructure takes care of all encryption and decryption
work. One exception is SSL to the client, assuming you have ``hive.s3.ssl.enabled`` set to ``true``.
S3 also manages all the encryption keys for you. To enable this, set ``hive.s3.sse.enabled`` to ``true``.

With `S3 client-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html>`_,
S3 stores encrypted data and the encryption keys are managed outside of the S3 infrastructure. Data is encrypted
and decrypted by Trino instead of in the S3 infrastructure. In this case, encryption keys can be managed
either by using the AWS KMS, or your own key management system. To use the AWS KMS for key management, set
``hive.s3.kms-key-id`` to the UUID of a KMS key. Your AWS credentials or EC2 IAM role will need to be
granted permission to use the given key as well.

To use a custom encryption key management system, set ``hive.s3.encryption-materials-provider`` to the
fully qualified name of a class which implements the
`EncryptionMaterialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/EncryptionMaterialsProvider.html>`_
interface from the AWS Java SDK. This class has to be accessible to the Hive Connector through the
classpath and must be able to communicate with your custom key management system. If this class also implements
the ``org.apache.hadoop.conf.Configurable`` interface from the Hadoop Java API, then the Hadoop configuration
is passed in after the object instance is created, and before it is asked to provision or retrieve any
encryption keys.

.. _s3selectpushdown:

S3 Select pushdown
------------------

S3 Select pushdown enables pushing down projection (SELECT) and predicate (WHERE)
processing to `S3 Select <https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html>`_.
With S3 Select Pushdown, Trino only retrieves the required data from S3 instead
of entire S3 objects, reducing both latency and network usage.

Is S3 Select a good fit for my workload?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Performance of S3 Select pushdown depends on the amount of data filtered by the
query. Filtering a large number of rows should result in better performance. If
the query doesn't filter any data, then pushdown may not add any additional value
and the user is charged for S3 Select requests. Thus, we recommend that you
benchmark your workloads with and without S3 Select to see if using it may be
suitable for your workload. By default, S3 Select Pushdown is disabled and you
should enable it in production after proper benchmarking and cost analysis. For
more information on S3 Select request cost, please see
`Amazon S3 Cloud Storage Pricing <https://aws.amazon.com/s3/pricing/>`_.

Use the following guidelines to determine if S3 Select is a good fit for your
workload:

* Your query filters out more than half of the original data set.
* Your query filter predicates use columns that have a data type supported by
  Trino and S3 Select.
  The ``TIMESTAMP``, ``REAL``, and ``DOUBLE`` data types are not supported by S3
  Select Pushdown. We recommend using the decimal data type for numerical data.
  For more information about supported data types for S3 Select, see the
  `Data Types documentation <https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-data-types.html>`_.
* Your network connection between Amazon S3 and the Amazon EMR cluster has good
  transfer speed and available bandwidth. Amazon S3 Select does not compress
  HTTP responses, so the response size may increase for compressed input files.

Considerations and limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Only objects stored in CSV format are supported. Objects can be uncompressed,
  or optionally compressed with gzip or bzip2.
* The "AllowQuotedRecordDelimiters" property is not supported. If this property
  is specified, the query fails.
* Amazon S3 server-side encryption with customer-provided encryption keys
  (SSE-C) and client-side encryption are not supported.
* S3 Select Pushdown is not a substitute for using columnar or compressed file
  formats such as ORC and Parquet.

Enabling S3 Select pushdown
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can enable S3 Select Pushdown using the ``s3_select_pushdown_enabled``
Hive session property, or using the ``hive.s3select-pushdown.enabled``
configuration property. The session property overrides the config
property, allowing you enable or disable on a per-query basis.

Understanding and tuning the maximum connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino can use its native S3 file system or EMRFS. When using the native FS, the
maximum connections is configured via the ``hive.s3.max-connections``
configuration property. When using EMRFS, the maximum connections is configured
via the ``fs.s3.maxConnections`` Hadoop configuration property.

S3 Select Pushdown bypasses the file systems, when accessing Amazon S3 for
predicate operations. In this case, the value of
``hive.s3select-pushdown.max-connections`` determines the maximum number of
client connections allowed for those operations from worker nodes.

If your workload experiences the error *Timeout waiting for connection from
pool*, increase the value of both ``hive.s3select-pushdown.max-connections`` and
the maximum connections configuration for the file system you are using.
