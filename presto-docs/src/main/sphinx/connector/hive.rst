==============
Hive Connector
==============

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The Hive connector allows querying data stored in an
`Apache Hive <https://hive.apache.org/>`_
data warehouse. Hive is a combination of three components:

* Data files in varying formats, that are typically stored in the
  Hadoop Distributed File System (HDFS) or in Amazon S3.
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database, such as MySQL, and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Supported File Types
--------------------

The following file types are supported for the Hive connector:

* ORC
* Parquet
* Avro
* RCText (RCFile using ``ColumnarSerDe``)
* RCBinary (RCFile using ``LazyBinaryColumnarSerDe``)
* SequenceFile
* JSON (using ``org.apache.hive.hcatalog.data.JsonSerDe``)
* CSV (using ``org.apache.hadoop.hive.serde2.OpenCSVSerde``)
* TextFile

Metastore Configuration for Avro and CSV
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to enable first-class support for Avro tables and CSV files when using
Hive 3.x, you need to add the following property definition to the Hive metastore
configuration file ``hive-site.xml`` (and restart the metastore service):

.. code-block:: xml

   <property>
        <!-- https://community.hortonworks.com/content/supportkb/247055/errorjavalangunsupportedoperationexception-storage.html -->
        <name>metastore.storage.schema.reader.impl</name>
        <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
    </property>

Supported Table Types
---------------------

Transactional and ACID Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When connecting to a Hive metastore version 3.x, the Hive connector supports reading
from insert-only and ACID tables, with full support for partitioning and bucketing.
Writing to and creation of transactional tables is not supported.

ACID tables created with `Hive Streaming Ingest <https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest>`_
are not supported.

Materialized Views
------------------

The Hive connector supports reading from Hive materialized views.
In Presto, these views are presented as regular, read-only tables.

Configuration
-------------

The Hive connector supports Apache Hadoop 2.x and derivative distributions
including Cloudera CDH 5 and Hortonworks Data Platform (HDP).

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive-hadoop2`` connector as the ``hive`` catalog,
replacing ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://example.net:9083

Multiple Hive Clusters
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For
example, if you name the property file ``sales.properties``, Presto
creates a catalog named ``sales`` using the configured connector.

HDFS Configuration
^^^^^^^^^^^^^^^^^^

For basic setups, Presto configures the HDFS client automatically and
does not require any configuration files. In some cases, such as when using
federated HDFS or NameNode high availability, it is necessary to specify
additional HDFS client options in order to access your HDFS cluster. To do so,
add the ``hive.config.resources`` property to reference your HDFS config files:

.. code-block:: none

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Only specify additional configuration files if necessary for your setup.
We recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

The configuration files must exist on all Presto nodes. If you are
referencing existing Hadoop config files, make sure to copy them to
any Presto nodes that are not running Hadoop.

HDFS Username and Permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before running any ``CREATE TABLE`` or ``CREATE TABLE AS`` statements
for Hive tables in Presto, you need to check that the user Presto is
using to access HDFS has access to the Hive warehouse directory. The Hive
warehouse directory is specified by the configuration variable
``hive.metastore.warehouse.dir`` in ``hive-site.xml``, and the default
value is ``/user/hive/warehouse``.

When not using Kerberos with HDFS, Presto accesses HDFS using the
OS user of the Presto process. For example, if Presto is running as
``nobody``, it accesses HDFS as ``nobody``. You can override this
username by setting the ``HADOOP_USER_NAME`` system property in the
Presto :ref:`presto_jvm_config`, replacing ``hdfs_user`` with the
appropriate username:

.. code-block:: none

    -DHADOOP_USER_NAME=hdfs_user

The ``hive`` user generally works, since Hive is often started with
the ``hive`` user and this user has access to the Hive warehouse.

Whenever you change the user Presto is using to access HDFS, remove
``/tmp/presto-*`` on HDFS, as the new user may not have access to
the existing temporary directories.

Accessing Hadoop clusters protected with Kerberos authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kerberos authentication is supported for both HDFS and the Hive metastore.
However, Kerberos authentication by ticket cache is not yet supported.

The properties that apply to Hive connector security are listed in the
`Hive Configuration Properties`_ table. Please see the
:doc:`/connector/hive-security` section for a more detailed discussion of the
security options in the Hive connector.

Hive Configuration Properties
-----------------------------

================================================== ============================================================ ============
Property Name                                      Description                                                  Default
================================================== ============================================================ ============
``hive.metastore``                                 The type of Hive metastore to use. Presto currently supports ``thrift``
                                                   the default Hive Thrift metastore (``thrift``), and the AWS
                                                   Glue Catalog (``glue``) as metadata sources.

``hive.config.resources``                          An optional comma-separated list of HDFS
                                                   configuration files. These files must exist on the
                                                   machines running Presto. Only specify this if
                                                   absolutely necessary to access HDFS.
                                                   Example: ``/etc/hdfs-site.xml``

``hive.recursive-directories``                     Enable reading data from subdirectories of table or          ``false``
                                                   partition locations. If disabled, subdirectories are
                                                   ignored. This is equivalent to the
                                                   ``hive.mapred.supports.subdirectories`` property in Hive.

``hive.ignore-absent-partitions``                  Ignore partitions when the file system location does not     ``false``
                                                   exist rather than failing the query. This skips data that
                                                   may be expected to be part of the table.

``hive.storage-format``                            The default file format used when creating new tables.       ``ORC``

``hive.compression-codec``                         The compression codec to use when writing files.             ``GZIP``

``hive.force-local-scheduling``                    Force splits to be scheduled on the same node as the Hadoop  ``false``
                                                   DataNode process serving the split data.  This is useful for
                                                   installations where Presto is collocated with every
                                                   DataNode.

``hive.respect-table-format``                      Should new partitions be written using the existing table    ``true``
                                                   format or the default Presto format?

``hive.immutable-partitions``                      Can new data be inserted into existing partitions?           ``false``

``hive.create-empty-bucket-files``                 Should empty files be created for buckets that have no data? ``false``

``hive.max-partitions-per-writers``                Maximum number of partitions per writer.                     100

``hive.max-partitions-per-scan``                   Maximum number of partitions for a single table scan.        100,000

``hive.hdfs.authentication.type``                  HDFS authentication type.                                    ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.hdfs.impersonation.enabled``                Enable HDFS end user impersonation.                          ``false``

``hive.hdfs.presto.principal``                     The Kerberos principal that Presto will use when connecting
                                                   to HDFS.

``hive.hdfs.presto.keytab``                        HDFS client keytab location.

``hive.security``                                  See :doc:`hive-security`.

``security.config-file``                           Path of config file to use when ``hive.security=file``.
                                                   See :ref:`hive-file-based-authorization` for details.

``hive.non-managed-table-writes-enabled``          Enable writes to non-managed (external) Hive tables.         ``false``

``hive.non-managed-table-creates-enabled``         Enable creating non-managed (external) Hive tables.          ``true``

``hive.collect-column-statistics-on-write``        Enables automatic column level statistics collection         ``true``
                                                   on write. See `Table Statistics <#table-statistics>`__ for
                                                   details.

``hive.s3select-pushdown.enabled``                 Enable query pushdown to AWS S3 Select service.              ``false``

``hive.s3select-pushdown.max-connections``         Maximum number of simultaneously open connections to S3 for  500
                                                   :ref:`s3selectpushdown`.

``hive.file-status-cache-tables``                  Cache directory listing for specified tables.
                                                   Examples: ``schema.table1,schema.table2`` to cache directory
                                                   listing only for ``table1`` and ``table2``.
                                                   ``schema1.*,schema2.*`` to cache directory listing for all
                                                   tables in the schemas ``schema1`` and ``schema2``.
                                                   ``*`` to cache directory listing for all tables.

``hive.file-status-cache-size``                    Maximum no. of file status entries cached for a path.        1,000,000

``hive.file-status-cache-expire-time``             Duration of time after a directory listing is cached that it ``1m``
                                                   should be automatically removed from cache.
================================================== ============================================================ ============

Hive Thrift Metastore Configuration Properties
----------------------------------------------

================================================== ============================================================ ============
Property Name                                      Description                                                  Default
================================================== ============================================================ ============
``hive.metastore.uri``                             The URI(s) of the Hive metastore to connect to using the
                                                   Thrift protocol. If multiple URIs are provided, the first
                                                   URI is used by default, and the rest of the URIs are
                                                   fallback metastores. This property is required.
                                                   Example: ``thrift://192.0.2.3:9083`` or
                                                   ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.metastore.username``                        The username Presto uses to access the Hive metastore.

``hive.metastore.authentication.type``             Hive metastore authentication type.
                                                   Possible values are ``NONE`` or ``KERBEROS``
                                                   (defaults to ``NONE``).

``hive.metastore.thrift.impersonation.enabled``    Enable Hive metastore end user impersonation.

``hive.metastore.service.principal``               The Kerberos principal of the Hive metastore service.

``hive.metastore.client.principal``                The Kerberos principal that Presto uses when connecting
                                                   to the Hive metastore service.

``hive.metastore.client.keytab``                   Hive metastore client keytab location.

``hive.metastore-cache-ttl``                       Time to live Hive metadata cache.                            ``0s``

``hive.metastore-refresh-interval``                How often to refresh the Hive metastore cache.

``hive.metastore-cache-maximum-size``              Hive metastore cache maximum size.                           10,000

``hive.metastore-refresh-max-threads``             Maximum number of threads to refresh Hive metastore cache.   100
================================================== ============================================================ ============

AWS Glue Catalog Configuration Properties
-----------------------------------------

In order to use a Glue catalog, ensure to configure the metastore with
``hive.metastore=glue`` and provide further details with the following
properties:

==================================================== ============================================================
Property Name                                        Description
==================================================== ============================================================
``hive.metastore.glue.region``                       AWS region of the Glue Catalog. This is required when not
                                                     running in EC2, or when the catalog is in a different region.
                                                     Example: ``us-east-1``

``hive.metastore.glue.pin-client-to-current-region`` Pin Glue requests to the same region as the EC2 instance
                                                     where Presto is running, defaults to ``false``.

``hive.metastore.glue.max-connections``              Max number of concurrent connections to Glue,
                                                     defaults to ``5``.

``hive.metastore.glue.default-warehouse-dir``        Hive Glue metastore default warehouse directory

``hive.metastore.glue.aws-access-key``               AWS access key to use to connect to the Glue Catalog. If
                                                     specified along with ``hive.metastore.glue.aws-secret-key``,
                                                     this parameter takes precedence over
                                                     ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.aws-secret-key``               AWS secret key to use to connect to the Glue Catalog. If
                                                     specified along with ``hive.metastore.glue.aws-access-key``,
                                                     this parameter takes precedence over
                                                     ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.iam-role``                     ARN of an IAM role to assume when connecting to the Glue
                                                     Catalog.

``hive.metastore.glue.external-id``                  External ID for the IAM role trust policy when connecting
                                                     to the Glue Catalog.
==================================================== ============================================================

.. _hive-s3:

Amazon S3 Configuration
-----------------------

The Hive Connector can read and write tables that are stored in S3.
This is accomplished by having a table or database location that
uses an S3 prefix, rather than an HDFS prefix.

Presto uses its own S3 filesystem for the URI prefixes
``s3://``, ``s3n://`` and  ``s3a://``.

S3 Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
                                             (e.g., ``http[s]://<bucket>.s3-<AWS-region>.amazonaws.com``).

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
                                             instance where Presto is running,
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
                                             to ``Private``.

``hive.s3.skip-glacier-objects``             Ignore Glacier objects rather than failing the query. This
                                             skips data that may be expected to be part of the table
                                             or partition. Defaults to ``false``.
============================================ =================================================================

.. _hive-s3-credentials:

S3 Credentials
^^^^^^^^^^^^^^

If you are running Presto on Amazon EC2, using EMR or another facility,
it is recommended that you use IAM Roles for EC2 to govern access to S3.
To enable this, your EC2 instances need to be assigned an IAM Role which
grants appropriate access to the data stored in the S3 bucket(s) you wish
to use. It is also possible to configure an IAM role with ``hive.s3.iam-role``
that is used for accessing any S3 bucket. This is much cleaner than
setting AWS access and secret keys in the ``hive.s3.aws-access-key``
and ``hive.s3.aws-secret-key`` settings, and also allows EC2 to automatically
rotate credentials on a regular basis without any additional work on your part.

Custom S3 Credentials Provider
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can configure a custom S3 credentials provider by setting the Hadoop
configuration property ``presto.s3.credentials-provider`` to be the
fully qualified class name of a custom AWS credentials provider
implementation. This class must implement the
`AWSCredentialsProvider <http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html>`_
interface and provide a two-argument constructor that takes a
``java.net.URI`` and a Hadoop ``org.apache.hadoop.conf.Configuration``
as arguments. A custom credentials provider can be used to provide
temporary credentials from STS (using ``STSSessionCredentialsProvider``),
IAM role-based credentials (using ``STSAssumeRoleSessionCredentialsProvider``),
or credentials for a specific use case (e.g., bucket/user specific credentials).
This Hadoop configuration property must be set in the Hadoop configuration
files referenced by the ``hive.config.resources`` Hive connector property.

.. _hive-s3-security-mapping:

S3 Security Mapping
^^^^^^^^^^^^^^^^^^^

Presto supports flexible security mapping for S3, allowing for separate
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

* ``allowedIamRoles``: IAM roles that are allowed to be specified as an extra
  credential. This is useful because a particular AWS account may have permissions
  to use many roles, but a specific user should only be allowed to use a subset
  of those roles.

The security mapping entries are processed in the order listed in the configuration
file. More specific mappings should thus be specified before less specific mappings.
For example, the mapping list might have URL prefix ``s3://abc/xyz/`` followed by
``s3://abc/`` to allow different configuration for a specific path within a bucket
than for other paths within the bucket. You can set default configuration by not
including any match criteria for the last entry in the list.

Example JSON configuration file:

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
``hive.s3.security-mapping.config-file``                The JSON configuration file containing security mappings.

``hive.s3.security-mapping.iam-role-credential-name``   The name of the *extra credential* used to provide the IAM role.

``hive.s3.security-mapping.refresh-period``             How often to refresh the security mapping configuration.

``hive.s3.security-mapping.colon-replacement``          The character or characters to be used in place of the colon
                                                        (``:``) character when specifying an IAM role name as an
                                                        extra credential. Any instances of this replacement value in the
                                                        extra credential value will be converted to a colon. Choose a
                                                        value that is not used in any of your IAM ARNs.
======================================================= =================================================================

Tuning Properties
^^^^^^^^^^^^^^^^^

The following tuning properties affect the behavior of the client
used by the Presto S3 filesystem when communicating with S3.
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

S3 Data Encryption
^^^^^^^^^^^^^^^^^^

Presto supports reading and writing encrypted data in S3 using both
server-side encryption with S3 managed keys and client-side encryption using
either the Amazon KMS or a software plugin to manage AES encryption keys.

With `S3 server-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html>`_,
called *SSE-S3* in the Amazon documentation, the S3 infrastructure takes care of all encryption and decryption
work. One exception is SSL to the client, assuming you have ``hive.s3.ssl.enabled`` set to ``true``.
S3 also manages all the encryption keys for you. To enable this, set ``hive.s3.sse.enabled`` to ``true``.

With `S3 client-side encryption <http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html>`_,
S3 stores encrypted data and the encryption keys are managed outside of the S3 infrastructure. Data is encrypted
and decrypted by Presto instead of in the S3 infrastructure. In this case, encryption keys can be managed
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

S3 Select Pushdown
^^^^^^^^^^^^^^^^^^

S3 Select Pushdown enables pushing down projection (SELECT) and predicate (WHERE)
processing to `S3 Select <https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html>`_.
With S3 Select Pushdown, Presto only retrieves the required data from S3 instead
of entire S3 objects, reducing both latency and network usage.

Is S3 Select a good fit for my workload?
########################################

Performance of S3 Select Pushdown depends on the amount of data filtered by the
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
  Presto and S3 Select.
  The ``TIMESTAMP``, ``REAL``, and ``DOUBLE`` data types are not supported by S3
  Select Pushdown. We recommend using the decimal data type for numerical data.
  For more information about supported data types for S3 Select, see the
  `Data Types documentation <https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-data-types.html>`_.
* Your network connection between Amazon S3 and the Amazon EMR cluster has good
  transfer speed and available bandwidth. Amazon S3 Select does not compress
  HTTP responses, so the response size may increase for compressed input files.

Considerations and Limitations
##############################

* Only objects stored in CSV format are supported. Objects can be uncompressed,
  or optionally compressed with gzip or bzip2.
* The "AllowQuotedRecordDelimiters" property is not supported. If this property
  is specified, the query fails.
* Amazon S3 server-side encryption with customer-provided encryption keys
  (SSE-C) and client-side encryption are not supported.
* S3 Select Pushdown is not a substitute for using columnar or compressed file
  formats such as ORC and Parquet.

Enabling S3 Select Pushdown
###########################

You can enable S3 Select Pushdown using the ``s3_select_pushdown_enabled``
Hive session property, or using the ``hive.s3select-pushdown.enabled``
configuration property. The session property overrides the config
property, allowing you enable or disable on a per-query basis.

Understanding and Tuning the Maximum Connections
################################################

Presto can use its native S3 file system or EMRFS. When using the native FS, the
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

Google Cloud Storage Configuration
----------------------------------

The Hive connector can access data stored in GCS, using the ``gs://`` URI prefix.
Please refer to the :doc:`hive-gcs-tutorial` for step-by-step instructions.

GCS Configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

============================================ =================================================================
Property Name                                Description
============================================ =================================================================
``hive.gcs.json-key-file-path``              JSON key file used to authenticate with Google Cloud Storage.

``hive.gcs.use-access-token``                Use client-provided OAuth token to access Google Cloud Storage.
                                             This is mutually exclusive with a global JSON key file.
============================================ =================================================================

Alluxio Configuration
---------------------

Presto can read and write tables stored in the
`Alluxio Data Orchestration System <https://www.alluxio.io/?utm_source=prestosql&utm_medium=prestodocs>`_,
leveraging Alluxio's distributed block-level read/write caching functionality.
The tables must be created in the Hive metastore with the ``alluxio://`` location prefix
(see `Running Apache Hive with Alluxio <https://docs.alluxio.io/os/user/2.1/en/compute/Hive.html?utm_source=prestosql&utm_medium=prestodocs>`_
for details and examples).
Presto queries will then transparently retrieve and cache files
or objects from a variety of disparate storage systems including HDFS and S3.

Alluxio Client-Side Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To configure Alluxio client-side properties on Presto, append the Alluxio
configuration directory (``${ALLUXIO_HOME}/conf``) to the Presto JVM classpath,
so that the Alluxio properties file ``alluxio-site.properties`` can be loaded as a resource.
Update the Presto :ref:`presto_jvm_config` file ``etc/jvm.config`` to include the following:

.. code-block:: none

  -Xbootclasspath/a:<path-to-alluxio-conf>

The advantage of this approach is that all the Alluxio properties are set in
the single ``alluxio-site.properties`` file. For details, see `Customize Alluxio User Properties
<https://docs.alluxio.io/os/user/2.1/en/compute/Presto.html#customize-alluxio-user-properties?utm_source=prestosql&utm_medium=prestodocs>`_.

Alternatively, add Alluxio configuration properties to the Hadoop configuration
files (``core-site.xml``, ``hdfs-site.xml``) and configure the Hive connector
to use the `Hadoop configuration files <#hdfs-configuration>`__ via the
``hive.config.resources`` connector property.

Deploy Alluxio with Presto
^^^^^^^^^^^^^^^^^^^^^^^^^^

To achieve the best performance running Presto on Alluxio, it is recommended
to collocate Presto workers with Alluxio workers. This allows reads and writes
to bypass the network (*short-circuit*). See `Performance Tuning Tips for Presto with Alluxio
<https://www.alluxio.io/blog/top-5-performance-tuning-tips-for-running-presto-on-alluxio-1/?utm_source=prestosql&utm_medium=prestodocs>`_
for more details.

Alluxio Catalog Service
^^^^^^^^^^^^^^^^^^^^^^^

An alternative way for Presto to interact with Alluxio is via the
`Alluxio catalog service <https://docs.alluxio.io/os/user/stable/en/core-services/Catalog.html?utm_source=prestosql&utm_medium=prestodocs>`_.
The primary benefits for using the Alluxio catalog service are simpler
deployment of Alluxio with Presto, and enabling schema-aware optimizations
such as transparent caching and transformations. Currently, the catalog service
supports read-only workloads.

The Alluxio catalog service is a metastore that can cache the information
from different underlying metastores. It currently supports the Hive metastore
as an underlying metastore. In order for the Alluxio catalog to manage the metadata
of other existing metastores, the other metastores must be "attached" to the
Alluxio catalog. To attach an existing Hive metastore to the Alluxio
catalog, simply use the
`Alluxio CLI attachdb command <https://docs.alluxio.io/os/user/stable/en/operation/User-CLI.html?utm_source=prestosql&utm_medium=prestodocs#attachdb>`_.
The appropriate Hive metastore location and Hive database name need to be
provided.

.. code-block:: none

    ./bin/alluxio table attachdb hive thrift://HOSTNAME:9083 hive_db_name

Once a metastore is attached, the Alluxio catalog can manage and serve the
information to Presto. To configure the Hive connector for Alluxio
catalog service, simply configure the connector to use the Alluxio
metastore type, and provide the location to the Alluxio cluster.
For example, your ``etc/catalog/alluxio.properties`` should include
the following:

.. code-block:: none

    connector.name=hive-hadoop2
    hive.metastore=alluxio
    hive.metastore.alluxio.master.address=HOSTNAME:PORT

Replace ``HOSTNAME`` with the Alluxio master hostname, and replace ``PORT``
with the Alluxio master port.
An example of an Alluxio master address is ``master-node:19998``.
Now, Presto queries can take advantage of the Alluxio catalog service, such as
transparent caching and transparent transformations, without any modifications
to existing Hive metastore deployments.

Table Statistics
----------------

When writing data, the Hive connector always collects basic statistics
(``numFiles``, ``numRows``, ``rawDataSize``, ``totalSize``)
and by default will also collect column level statistics:

============= ====================================================================
Column Type   Collectible Statistics
============= ====================================================================
``TINYINT``   number of nulls, number of distinct values, min/max values
``SMALLINT``  number of nulls, number of distinct values, min/max values
``INTEGER``   number of nulls, number of distinct values, min/max values
``BIGINT``    number of nulls, number of distinct values, min/max values
``DOUBLE``    number of nulls, number of distinct values, min/max values
``REAL``      number of nulls, number of distinct values, min/max values
``DECIMAL``   number of nulls, number of distinct values, min/max values
``DATE``      number of nulls, number of distinct values, min/max values
``TIMESTAMP`` number of nulls, number of distinct values, min/max values
``VARCHAR``   number of nulls, number of distinct values
``CHAR``      number of nulls, number of distinct values
``VARBINARY`` number of nulls
``BOOLEAN``   number of nulls, number of true/false values
============= ====================================================================

.. _hive_analyze:

Updating table and partition statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If your queries are complex and include joining large data sets,
running :doc:`/sql/analyze` on tables/partitions may improve query performance
by collecting statistical information about the data.

When analyzing a partitioned table, the partitions to analyze can be specified
via the optional ``partitions`` property, which is an array containing
the values of the partition keys in the order they are declared in the table schema::

    ANALYZE table_name WITH (
        partitions = ARRAY[
            ARRAY['p1_value1', 'p1_value2'],
            ARRAY['p2_value1', 'p2_value2']])

This query will collect statistics for two partitions with keys
``p1_value1, p1_value2`` and ``p2_value1, p2_value2``.

On wide tables, collecting statistics for all columns can be expensive and can have a
detrimental effect on query planning. It is also typically unnecessary - statistics are
only useful on specific columns, like join keys, predicates, grouping keys. One can
specify a subset of columns to be analyzed via the optional ``columns`` property::

    ANALYZE table_name WITH (
        partitions = ARRAY[ARRAY['p2_value1', 'p2_value2']],
        columns = ARRAY['col_1', 'col_2'])

This query collects statistics for columns ``col_1`` and ``col_2`` for the partition
with keys ``p2_value1, p2_value2``.

Note that if statistics were previously collected for all columns, they need to be dropped
before re-analyzing just a subset::

    CALL system.drop_stats(schema_name, table_name, ARRAY[ARRAY['p2_value1', 'p2_value2']])

Schema Evolution
----------------

Hive allows the partitions in a table to have a different schema than the
table. This occurs when the column types of a table are changed after
partitions already exist (that use the original column types). The Hive
connector supports this by allowing the same conversions as Hive:

* ``varchar`` to and from ``tinyint``, ``smallint``, ``integer`` and ``bigint``
* ``real`` to ``double``
* Widening conversions for integers, such as ``tinyint`` to ``smallint``

Any conversion failure results in null, which is the same behavior
as Hive. For example, converting the string ``'foo'`` to a number,
or converting the string ``'1234'`` to a ``tinyint`` (which has a
maximum value of ``127``).

Avro Schema Evolution
---------------------

Presto supports querying and manipulating Hive tables with the Avro storage
format, which has the schema set based on an Avro schema file/literal. Presto is
also capable of creating the tables in Presto by infering the schema from a
valid Avro schema file located locally, or remotely in HDFS/Web server.

To specify that the Avro schema should be used for interpreting table's data one must use ``avro_schema_url`` table property.
The schema can be placed remotely in
HDFS (e.g. ``avro_schema_url = 'hdfs://user/avro/schema/avro_data.avsc'``),
S3 (e.g. ``avro_schema_url = 's3n:///schema_bucket/schema/avro_data.avsc'``),
a web server (e.g. ``avro_schema_url = 'http://example.org/schema/avro_data.avsc'``)
as well as local file system. This URL, where the schema is located, must be accessible from the
Hive metastore and Presto coordinator/worker nodes.

The table created in Presto using ``avro_schema_url`` behaves the same way as a Hive table with ``avro.schema.url`` or ``avro.schema.literal`` set.

Example::

   CREATE TABLE hive.avro.avro_data (
      id bigint
    )
   WITH (
      format = 'AVRO',
      avro_schema_url = '/usr/local/avro_data.avsc'
   )

The columns listed in the DDL (``id`` in the above example) is ignored if ``avro_schema_url`` is specified.
The table schema matches the schema in the Avro schema file. Before any read operation, the Avro schema is
accessed so the query result reflects any changes in schema. Thus Presto takes advantage of Avro's backward compatibility abilities.

If the schema of the table changes in the Avro schema file, the new schema can still be used to read old data.
Newly added/renamed fields *must* have a default value in the Avro schema file.

The schema evolution behavior is as follows:

* Column added in new schema:
  Data created with an older schema produces a *default* value when table is using the new schema.

* Column removed in new schema:
  Data created with an older schema no longer outputs the data from the column that was removed.

* Column is renamed in the new schema:
  This is equivalent to removing the column and adding a new one, and data created with an older schema
  produces a *default* value when table is using the new schema.

* Changing type of column in the new schema:
  If the type coercion is supported by Avro or the Hive connector, then the conversion happens.
  An error is thrown for incompatible types.

Limitations
^^^^^^^^^^^

The following operations are not supported when ``avro_schema_url`` is set:

* ``CREATE TABLE AS`` is not supported.
* Using partitioning(``partitioned_by``) or bucketing(``bucketed_by``) columns are not supported in ``CREATE TABLE``.
* ``ALTER TABLE`` commands modifying columns are not supported.

Procedures
----------

* ``system.create_empty_partition(schema_name, table_name, partition_columns, partition_values)``

    Create an empty partition in the specified table.

* ``system.sync_partition_metadata(schema_name, table_name, mode)``

    Check and update partitions list in metastore. There are three modes available:

    * ``ADD`` : add any partitions that exist on the file system, but not in the metastore.
    * ``DROP``: drop any partitions that exist in the metastore, but not on the file system.
    * ``FULL``: perform both ``ADD`` and ``DROP``.

* ``system.drop_stats(schema_name, table_name, partition_values)``

    Drops statistics for a subset of partitions or the entire table. The partitions are specified as an
    array whose elements are arrays of partition values (similar to the ``partition_values`` argument in
    ``create_empty_partition``). A null value for the ``partition_values`` argument indicates that stats
    should be dropped for the entire table.

.. _register_partition:

* ``system.register_partition(schema_name, table_name, partition_columns, partition_values, location)``

    Registers existing location as a new partition in the metastore for the specified table.

    Due to security reasons, the procedure is enabled only when ``hive.allow-register-partition-procedure``
    is set to ``true``.

.. _unregister_partition:

* ``system.unregister_partition(schema_name, table_name, partition_columns, partition_values)``

    Unregisters given, existing partition in the metastore for the specified table.
    The partition data is not deleted.

Special Columns
---------------

In addition to the defined columns, the Hive connector automatically exposes
metadata in a number of hidden columns in each table. You can use these columns
in your SQL statements like any other column, e.g., they can be selected
directly or used in conditional statements.

* ``$bucket``
    Bucket number for this row.

* ``$path``
    Full file system path name of the file for this row.

* ``$file_modified_time``
    Date and time of the last modification of the file for this row.

* ``$file_size``
    Size of the file for this row.

Special Tables
----------------

Table Properties
^^^^^^^^^^^^^^^^

The raw Hive table properties are available as a hidden table, containing a
separate column per table property, with a single row containing the property
values. The properties table name is the same as the table name with
``$properties`` appended.

You can inspect the property names and values with a simple query::

    SELECT * FROM hive.web."page_views$properties";

Examples
--------

The Hive connector supports querying and manipulating Hive tables and schemas
(databases). While some uncommon operations need to be performed using
Hive directly, most operations can be performed using Presto.

Create a new Hive schema named ``web`` that stores tables in an
S3 bucket named ``my-bucket``::

    CREATE SCHEMA hive.web
    WITH (location = 's3://my-bucket/')

Create a new Hive table named ``page_views`` in the ``web`` schema
that is stored using the ORC file format, partitioned by date and
country, and bucketed by user into ``50`` buckets. Note that Hive
requires the partition columns to be the last columns in the table::

    CREATE TABLE hive.web.page_views (
      view_time timestamp,
      user_id bigint,
      page_url varchar,
      ds date,
      country varchar
    )
    WITH (
      format = 'ORC',
      partitioned_by = ARRAY['ds', 'country'],
      bucketed_by = ARRAY['user_id'],
      bucket_count = 50
    )

Drop a partition from the ``page_views`` table::

    DELETE FROM hive.web.page_views
    WHERE ds = DATE '2016-08-09'
      AND country = 'US'

Add an empty partition to the ``page_views`` table::

    CALL system.create_empty_partition(
        schema_name => 'web',
        table_name => 'page_views',
        partition_columns => ARRAY['ds', 'country'],
        partition_values => ARRAY['2016-08-09', 'US']);

Drop stats for a partition of the ``page_views`` table::

    CALL system.drop_stats(
        schema_name => 'web',
        table_name => 'page_views',
        partition_values => ARRAY['2016-08-09', 'US']);

Query the ``page_views`` table::

    SELECT * FROM hive.web.page_views

List the partitions of the ``page_views`` table::

    SELECT * FROM hive.web."page_views$partitions"

Create an external Hive table named ``request_logs`` that points at
existing data in S3::

    CREATE TABLE hive.web.request_logs (
      request_time timestamp,
      url varchar,
      ip varchar,
      user_agent varchar
    )
    WITH (
      format = 'TEXTFILE',
      external_location = 's3://my-bucket/data/logs/'
    )

Collect statistics for the ``request_logs`` table::

    ANALYZE hive.web.request_logs;

The examples shown here should work on Google Cloud Storage after replacing ``s3://`` with ``gs://``.

Cleaning up
^^^^^^^^^^^

Drop the external table ``request_logs``. This only drops the metadata
for the table. The referenced data directory is not deleted::

    DROP TABLE hive.web.request_logs

Drop a schema::

    DROP SCHEMA hive.web

Hive Connector Limitations
--------------------------

* :doc:`/sql/delete` is only supported if the ``WHERE`` clause matches entire partitions.
* :doc:`/sql/alter-schema` usage fails, since the Hive metastore does not support renaming schemas.

Hive 3 Related Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^

* For security reasons, the ``sys`` system catalog is not accessible.
* Hive's ``timestamp with local zone`` data type is not supported.
  It is possible to read from a table with a column of this type, but the column
  data is not accessible. Writing to such a table is not supported.
* Due to Hive issues `HIVE-21002 <https://issues.apache.org/jira/browse/HIVE-21002>`_
  and `HIVE-22167 <https://issues.apache.org/jira/browse/HIVE-22167>`_, Presto does
  not correctly read ``timestamp`` values from Parquet, RCBinary, or Avro
  file formats created by Hive 3.1 or later. When reading from these file formats,
  Presto returns different results than Hive.
