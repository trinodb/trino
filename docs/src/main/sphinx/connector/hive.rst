==============
Hive connector
==============

.. toctree::
    :maxdepth: 1
    :hidden:

    Security <hive-security>
    Amazon S3 <hive-s3>
    Azure Storage <hive-azure>
    GCS Tutorial <hive-gcs-tutorial>
    Storage Caching <hive-caching>
    Alluxio <hive-alluxio>

Overview
--------

The Hive connector allows querying data stored in an
`Apache Hive <https://hive.apache.org/>`_
data warehouse. Hive is a combination of three components:

* Data files in varying formats, that are typically stored in the
  Hadoop Distributed File System (HDFS) or in object storage systems
  such as Amazon S3.
* Metadata about how the data files are mapped to schemas and tables. This
  metadata is stored in a database, such as MySQL, and is accessed via the Hive
  metastore service.
* A query language called HiveQL. This query language is executed on a
  distributed computing framework such as MapReduce or Tez.

Trino only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Requirements
------------

The Hive connector requires a Hive metastore service (HMS), or a compatible
implementation of the Hive metastore, such as
`AWS Glue Data Catalog <https://aws.amazon.com/glue/>`_.

Apache Hadoop 2.x and 3.x are supported, along with derivative distributions,
including Cloudera CDH 5 and Hortonworks Data Platform (HDP).

Many distributed storage systems including HDFS,
:doc:`Amazon S3 <hive-s3>` or S3-compatible systems,
`Google Cloud Storage <#google-cloud-storage-configuration>`__,
and :doc:`Azure Storage <hive-azure>` can be queried with the Hive connector.

The coordinator and all workers must have network access to the Hive metastore
and the storage system. Hive metastore access with the Thrift protocol defaults
to using port 9083.

Supported file types
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

Metastore configuration for Avro
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to enable first-class support for Avro tables when using
Hive 3.x, you need to add the following property definition to the Hive metastore
configuration file ``hive-site.xml`` (and restart the metastore service):

.. code-block:: xml

   <property>
        <!-- https://community.hortonworks.com/content/supportkb/247055/errorjavalangunsupportedoperationexception-storage.html -->
        <name>metastore.storage.schema.reader.impl</name>
        <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
    </property>

Materialized views
------------------

The Hive connector supports reading from Hive materialized views.
In Trino, these views are presented as regular, read-only tables.

.. _hive-views:

Hive views
----------

Hive views are defined in HiveQL and stored in the Hive Metastore Service. They
are analyzed to allow read access to the data.

The Hive connector includes support for reading Hive views with three different
modes.

* Disabled
* Legacy
* Experimental

You can configure the behavior in your catalog properties file.

**Disabled**

The default behavior is to ignore Hive views. This means that your business
logic and data encoded in the views is not available in Trino.

**Legacy**

A very simple implementation to execute Hive views, and therefore allow read
access to the data in Trino, can be enabled with
``hive.translate-hive-views=true`` and
``hive.legacy-hive-view-translation=true``.

For temporary usage of the legacy behavior for a specific catalog, you can set
the ``legacy_hive_view_translation`` :doc:`catalog session property
</sql/set-session>` to ``true``.

This legacy behavior interprets any HiveQL query that defines a view as if it
is written in SQL. It does not do any translation, but instead relies on the
fact that HiveQL is very similar to SQL.

This works for very simple Hive views, but can lead to problems for more complex
queries. For example, if a HiveQL function has an identical signature but
different behaviors to the SQL version, the returned results may differ. In more
extreme cases the queries might fail, or not even be able to be parsed and
executed.

**Experimental**

The new behavior is better engineered and has the potential to become a lot
more powerful than the legacy implementation. It can analyze, process, and
rewrite Hive views and contained expressions and statements.

It supports the following Hive view functionality:

* ``UNION [DISTINCT]`` and ``UNION ALL`` against Hive views
* Nested ``GROUP BY`` clauses
* ``current_user()``
* ``LATERAL VIEW OUTER EXPLODE``
* ``LATERAL VIEW [OUTER] EXPLODE`` on array of struct
* ``LATERAL VIEW json_tuple``

You can enable the experimental behavior with
``hive.translate-hive-views=true``. Remove the
``hive.legacy-hive-view-translation`` property or set it to ``false`` to make
sure legacy is not enabled.

Keep in mind that numerous features are not yet implemented when experimenting
with this feature. The following is an incomplete list of **missing**
functionality:

* HiveQL ``current_date``, ``current_timestamp``, and others
* Hive function calls including ``translate()``, window functions, and others
* Common table expressions and simple case expressions
* Honor timestamp precision setting
* Support all Hive data types and correct mapping to Trino types
* Ability to process custom UDFs

Configuration
-------------

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive`` connector as the ``hive`` catalog,
replacing ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: text

    connector.name=hive
    hive.metastore.uri=thrift://example.net:9083

Multiple Hive clusters
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``. For
example, if you name the property file ``sales.properties``, Trino
creates a catalog named ``sales`` using the configured connector.

HDFS configuration
^^^^^^^^^^^^^^^^^^

For basic setups, Trino configures the HDFS client automatically and
does not require any configuration files. In some cases, such as when using
federated HDFS or NameNode high availability, it is necessary to specify
additional HDFS client options in order to access your HDFS cluster. To do so,
add the ``hive.config.resources`` property to reference your HDFS config files:

.. code-block:: text

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Only specify additional configuration files if necessary for your setup.
We recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

The configuration files must exist on all Trino nodes. If you are
referencing existing Hadoop config files, make sure to copy them to
any Trino nodes that are not running Hadoop.

HDFS username and permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before running any ``CREATE TABLE`` or ``CREATE TABLE AS`` statements
for Hive tables in Trino, you need to check that the user Trino is
using to access HDFS has access to the Hive warehouse directory. The Hive
warehouse directory is specified by the configuration variable
``hive.metastore.warehouse.dir`` in ``hive-site.xml``, and the default
value is ``/user/hive/warehouse``.

When not using Kerberos with HDFS, Trino accesses HDFS using the
OS user of the Trino process. For example, if Trino is running as
``nobody``, it accesses HDFS as ``nobody``. You can override this
username by setting the ``HADOOP_USER_NAME`` system property in the
Trino :ref:`jvm_config`, replacing ``hdfs_user`` with the
appropriate username:

.. code-block:: text

    -DHADOOP_USER_NAME=hdfs_user

The ``hive`` user generally works, since Hive is often started with
the ``hive`` user and this user has access to the Hive warehouse.

Whenever you change the user Trino is using to access HDFS, remove
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

.. _hive_configuration_properties:

Hive configuration properties
-----------------------------

================================================== ============================================================ ============
Property Name                                      Description                                                  Default
================================================== ============================================================ ============
``hive.config.resources``                          An optional comma-separated list of HDFS
                                                   configuration files. These files must exist on the
                                                   machines running Trino. Only specify this if
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
                                                   Possible values are ``NONE``, ``SNAPPY``, ``LZ4``,
                                                   ``ZSTD``, or ``GZIP``.

``hive.force-local-scheduling``                    Force splits to be scheduled on the same node as the Hadoop  ``false``
                                                   DataNode process serving the split data.  This is useful for
                                                   installations where Trino is collocated with every
                                                   DataNode.

``hive.respect-table-format``                      Should new partitions be written using the existing table    ``true``
                                                   format or the default Trino format?

``hive.immutable-partitions``                      Can new data be inserted into existing partitions?           ``false``
                                                   If ``true`` then setting
                                                   ``hive.insert-existing-partitions-behavior`` to ``APPEND``
                                                   is not allowed.
                                                   This also affects the
                                                   ``insert_existing_partitions_behavior``
                                                   session property in the same way.

``hive.insert-existing-partitions-behavior``       What happens when data is inserted into an existing          ``APPEND``
                                                   partition?
                                                   Possible values are

                                                   * ``APPEND`` - appends data to existing partitions
                                                   * ``OVERWRITE`` - overwrites existing partitions
                                                   * ``ERROR`` - modifying existing partitions is not allowed

``hive.target-max-file-size``                      Best effort maximum size of new files.                       ``1GB``

``hive.create-empty-bucket-files``                 Should empty files be created for buckets that have no data? ``false``

``hive.partition-statistics-sample-size``          Specifies the number of partitions to analyze when           100
                                                   computing table statistics.

``hive.max-partitions-per-writers``                Maximum number of partitions per writer.                     100

``hive.max-partitions-per-scan``                   Maximum number of partitions for a single table scan.        100,000

``hive.hdfs.authentication.type``                  HDFS authentication type.                                    ``NONE``
                                                   Possible values are ``NONE`` or ``KERBEROS``.

``hive.hdfs.impersonation.enabled``                Enable HDFS end user impersonation.                          ``false``

``hive.hdfs.trino.principal``                      The Kerberos principal that Trino will use when connecting
                                                   to HDFS.

``hive.hdfs.trino.keytab``                         HDFS client keytab location.

``hive.dfs.replication``                           Hadoop file system replication factor.

``hive.security``                                  See :doc:`hive-security`.

``security.config-file``                           Path of config file to use when ``hive.security=file``.
                                                   See :ref:`catalog-file-based-access-control` for details.

``hive.non-managed-table-writes-enabled``          Enable writes to non-managed (external) Hive tables.         ``false``

``hive.non-managed-table-creates-enabled``         Enable creating non-managed (external) Hive tables.          ``true``

``hive.collect-column-statistics-on-write``        Enables automatic column level statistics collection         ``true``
                                                   on write. See `Table Statistics <#table-statistics>`__ for
                                                   details.

``hive.s3select-pushdown.enabled``                 Enable query pushdown to AWS S3 Select service.              ``false``

``hive.s3select-pushdown.max-connections``         Maximum number of simultaneously open connections to S3 for  500
                                                   :ref:`s3selectpushdown`.

``hive.file-status-cache-tables``                  Cache directory listing for specific tables. Examples:

                                                   * ``fruit.apple,fruit.orange`` to cache listings only for
                                                     tables ``apple`` and ``orange`` in schema ``fruit``
                                                   * ``fruit.*,vegetable.*`` to cache listings for all tables
                                                     in schemas ``fruit`` and ``vegetable``
                                                   * ``*`` to cache listings for all tables in all schemas

``hive.file-status-cache-size``                    Maximum total number of cached file status entries.          1,000,000

``hive.file-status-cache-expire-time``             How long a cached directory listing should be considered     ``1m``
                                                   valid.

``hive.rcfile.time-zone``                          Adjusts binary encoded timestamp values to a specific        JVM default
                                                   time zone. For Hive 3.1+, this should be set to UTC.

``hive.timestamp-precision``                       Specifies the precision to use for Hive columns of type      ``MILLISECONDS``
                                                   ``timestamp``. Possible values are ``MILLISECONDS``,
                                                   ``MICROSECONDS`` and ``NANOSECONDS``. Values with higher
                                                   precision than configured are rounded.

``hive.temporary-staging-directory-enabled``       Controls whether the temporary staging directory configured  ``true``
                                                   at ``hive.temporary-staging-directory-path`` should be
                                                   used for write operations. Temporary staging directory is
                                                   never used for writes to non-sorted tables on S3,
                                                   encrypted HDFS or external location. Writes to sorted tables
                                                   will utilize this path for staging temporary files
                                                   during sorting operation. When disabled, the target storage
                                                   will be used for staging while writing sorted tables which
                                                   can be inefficient when writing to object stores like S3.

``hive.temporary-staging-directory-path``          Controls the location of temporary staging directory that    ``/tmp/presto-${USER}``
                                                   is used for write operations. The ``${USER}`` placeholder
                                                   can be used to use a different location for each user.

``hive.translate-hive-views``                      Enable translation for :ref:`Hive views <hive-views>`.       ``false``

``hive.legacy-hive-view-translation``              Use the legacy algorithm to translate                        ``false``
                                                   :ref:`Hive views <hive-views>`. You can use the
                                                   ``legacy_hive_view_translation`` catalog session property
                                                   for temporary, catalog specific use.

``hive.parallel-partitioned-bucketed-writes``      Improve parallelism of partitioned and bucketed table        ``true``
                                                   writes. When disabled, the number of writing threads
                                                   is limited to number of buckets.

``hive.fs.new-directory-permissions``              Controls the permissions set on new directories created      ``0777``
                                                   for tables. It must be either 'skip' or an octal number,
                                                   with a leading 0. If set to 'skip', permissions of newly
                                                   created directories will not be set by Trino.

``hive.query-partition-filter-required``           Set to ``true`` to force a query to use a partition filter.   ``false``
                                                   You can use the ``query_partition_filter_required`` catalog
                                                   session property for temporary, catalog specific use.

``hive.table-statistics-enabled``                  Enables :doc:`/optimizer/statistics`. The equivalent          ``true``
                                                   :doc:`catalog session property </sql/set-session>`
                                                   is ``statistics_enabled`` for session specific use.
                                                   Set to ``false`` to disable statistics. Disabling statistics
                                                   means that :doc:`/optimizer/cost-based-optimizations` can
                                                   not make smart decisions about the query plan.
================================================== ============================================================ ============

ORC format configuration properties
-----------------------------------

The following properties are used to configure the read and write operations
with ORC files performed by the Hive connector.

.. list-table:: ORC format configuration properties
    :widths: 30, 50, 20
    :header-rows: 1

    * - Property Name
      - Description
      - Default
    * - ``hive.orc.time-zone``
      - Sets the default time zone for legacy ORC files that did not declare a
        time zone.
      - JVM default
    * - ``hive.orc.use-column-names``
      - Access ORC columns by name. By default, columns in ORC files are
        accessed by their ordinal position in the Hive table definition. The
        equivalent catalog session property is ``orc_use_column_names``.
      - ``false``

Parquet format configuration properties
---------------------------------------

The following properties are used to configure the read and write operations
with Parquet files performed by the Hive connector.

.. list-table:: Parquet format configuration properties
    :widths: 30, 50, 20
    :header-rows: 1

    * - Property Name
      - Description
      - Default
    * - ``hive.parquet.time-zone``
      - Adjusts timestamp values to a specific time zone. For Hive 3.1+, set
        this to UTC.
      - JVM default
    * - ``hive.parquet.use-columns-names``
      - Access Parquet columns by name by default. Set this property to
        ``false`` to access columns by their ordinal position in the Hive table
        definition. The equivalent catalog session property is
        ``parquet_use_column_names``.
      - ``true``


Metastore configuration properties
----------------------------------

The required Hive metastore can be configured with a number of properties.
Specific properties can be used to further configure the
`Thrift <#thrift-metastore-configuration-properties>`__ or
`Glue <#aws-glue-catalog-configuration-properties>`__ metastore.

======================================= =============================================================
Property Name                           Description
======================================= =============================================================
``hive.metastore``                      The type of Hive metastore to use. Trino currently supports
                                        the default Hive Thrift metastore (``thrift``), and the AWS
                                        Glue Catalog (``glue``) as metadata sources. Default is
                                        ``thrift``.

``hive.metastore-cache-ttl``            Duration how long cached metastore data should be considered
                                        valid. Default is ``0s``.

``hive.metastore-cache-maximum-size``   Maximum number of metastore data objects in the Hive
                                        metastore cache. Default is ``10000``.

``hive.metastore-refresh-interval``     Asynchronously refresh cached metastore data after access
                                        if it is older than this but is not yet expired, allowing
                                        subsequent accesses to see fresh data.

``hive.metastore-refresh-max-threads``  Maximum threads used to refresh cached metastore data.
                                        Default is ``10``.

``hive.metastore-timeout``              Timeout for Hive metastore requests. Default is ``10s``.

``hive.hide-delta-lake-tables``         Controls whether to hide Delta Lake tables in table
                                        listings. Currently applies only when using the AWS Glue
                                        metastore. Default is ``false``.
======================================= =============================================================

Thrift metastore configuration properties
-----------------------------------------

=============================================================== =============================================================
Property Name                                                   Description
=============================================================== =============================================================
``hive.metastore.uri``                                          The URIs of the Hive metastore to connect to using the
                                                                Thrift protocol. If a comma-separated list of URIs is
                                                                provided, the first URI is used by default, and the rest
                                                                of the URIs are fallback metastores. This property is required.
                                                                Example: ``thrift://192.0.2.3:9083`` or
                                                                ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.metastore.username``                                     The username Trino uses to access the Hive metastore.

``hive.metastore.authentication.type``                          Hive metastore authentication type.
                                                                Possible values are ``NONE`` or ``KERBEROS``.
                                                                Default is ``NONE``.

``hive.metastore.thrift.impersonation.enabled``                 Enable Hive metastore end user impersonation.

``hive.metastore.thrift.delegation-token.cache-ttl``            Time to live delegation token cache for metastore.
                                                                Default is ``1h``.

``hive.metastore.thrift.delegation-token.cache-maximum-size``   Delegation token cache maximum size. Default is ``1000``.

``hive.metastore.thrift.client.ssl.enabled``                    Use SSL when connecting to metastore. Default is ``false``.

``hive.metastore.thrift.client.ssl.key``                        Path to private key and client certificate (key store).

``hive.metastore.thrift.client.ssl.key-password``               Password for the private key.

``hive.metastore.thrift.client.ssl.trust-certificate``          Path to the server certificate chain (trust store).
                                                                Required when SSL is enabled.

``hive.metastore.thrift.client.ssl.trust-certificate-password`` Password for the trust store

``hive.metastore.service.principal``                            The Kerberos principal of the Hive metastore service.

``hive.metastore.client.principal``                             The Kerberos principal that Trino uses when connecting
                                                                to the Hive metastore service.

``hive.metastore.client.keytab``                                Hive metastore client keytab location.

=============================================================== =============================================================

AWS Glue catalog configuration properties
-----------------------------------------

In order to use a Glue catalog, ensure to configure the metastore with
``hive.metastore=glue`` and provide further details with the following
properties:

===================================================== ============================================================
Property Name                                         Description
===================================================== ============================================================
``hive.metastore.glue.region``                        AWS region of the Glue Catalog. This is required when not
                                                      running in EC2, or when the catalog is in a different region.
                                                      Example: ``us-east-1``

``hive.metastore.glue.endpoint-url``                  Glue API endpoint URL (optional).
                                                      Example: ``https://glue.us-east-1.amazonaws.com``

``hive.metastore.glue.pin-client-to-current-region``  Pin Glue requests to the same region as the EC2 instance
                                                      where Trino is running, defaults to ``false``.

``hive.metastore.glue.max-connections``               Max number of concurrent connections to Glue,
                                                      defaults to ``5``.

``hive.metastore.glue.max-error-retries``             Maximum number of error retries for the Glue client,
                                                      defaults to ``10``.

``hive.metastore.glue.default-warehouse-dir``         Default warehouse directory for schemas created without an
                                                      explicit ``location`` property.

``hive.metastore.glue.aws-credentials-provider``      Fully qualified name of the Java class to use for obtaining
                                                      AWS credentials. Can be used to supply a custom credentials
                                                      provider.

``hive.metastore.glue.aws-credentials-provider-conf`` Configuration details/file accepted by custom AwsCredentialsProvider
                                                      class.

``hive.metastore.glue.aws-access-key``                AWS access key to use to connect to the Glue Catalog. If
                                                      specified along with ``hive.metastore.glue.aws-secret-key``,
                                                      this parameter takes precedence over
                                                      ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.aws-secret-key``                AWS secret key to use to connect to the Glue Catalog. If
                                                      specified along with ``hive.metastore.glue.aws-access-key``,
                                                      this parameter takes precedence over
                                                      ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.catalogid``                     The ID of the Glue Catalog in which the metadata database
                                                      resides.

``hive.metastore.glue.iam-role``                      ARN of an IAM role to assume when connecting to the Glue
                                                      Catalog.

``hive.metastore.glue.external-id``                   External ID for the IAM role trust policy when connecting
                                                      to the Glue Catalog.

``hive.metastore.glue.partitions-segments``           Number of segments for partitioned Glue tables, defaults
                                                      to ``5``.

``hive.metastore.glue.get-partition-threads``         Number of threads for parallel partition fetches from Glue,
                                                      defaults to ``20``.

``hive.metastore.glue.read-statistics-threads``       Number of threads for parallel statistic fetches from Glue,
                                                      defaults to ``5``.

``hive.metastore.glue.write-statistics-threads``      Number of threads for parallel statistic writes to Glue,
                                                      defaults to ``5``.
===================================================== ============================================================

Google Cloud Storage configuration
----------------------------------

The Hive connector can access data stored in GCS, using the ``gs://`` URI prefix.
Please refer to the :doc:`hive-gcs-tutorial` for step-by-step instructions.

GCS configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

============================================ =================================================================
Property Name                                Description
============================================ =================================================================
``hive.gcs.json-key-file-path``              JSON key file used to authenticate with Google Cloud Storage.

``hive.gcs.use-access-token``                Use client-provided OAuth token to access Google Cloud Storage.
                                             This is mutually exclusive with a global JSON key file.
============================================ =================================================================

Performance tuning configuration properties
-------------------------------------------

The following table describes performance tuning properties for the Hive
connector.

.. warning::

   Performance tuning configuration properties are considered expert-level
   features. Altering these properties from their default values is likely to
   cause instability and performance degradation.

.. list-table::
    :widths: 30, 50, 20
    :header-rows: 1

    * - Property name
      - Description
      - Default value
    * - ``hive.max-outstanding-splits``
      - The target number of buffered splits for each table scan in a query,
        before the scheduler tries to pause.
      - ``1000``
    * - ``hive.max-splits-per-second``
      - The maximum number of splits generated per second per table scan. This
        can be used to reduce the load on the storage system. By default, there
        is no limit, which results in Presto maximizing the parallelization of
        data access.
      -
    * - ``hive.max-initial-splits``
      - For each table scan, the coordinator first assigns file sections of up
        to ``max-initial-split-size``. After ``max-initial-splits`` have been
        assigned, ``max-split-size`` is used for the remaining splits.
      - ``200``
    * - ``hive.max-initial-split-size``
      - The size of a single file section assigned to a worker until
        ``max-initial-splits`` have been assigned. Smaller splits results in
        more parallelism, which gives a boost to smaller queries.
      - ``32 MB``
    * - ``hive.max-split-size``
      - The largest size of a single file section assigned to a worker. Smaller
        splits result in more parallelism and thus can decrease latency, but
        also have more overhead and increase load on the system.
      - ``64 MB``

.. _hive-sql-support:

SQL support
-----------

The connector provides read access and write access to data and metadata in the
configured object storage system and metadata stores. In addition to the
:ref:`globally available <sql-globally-available>` and :ref:`read operation
<sql-read-operations>` statements, the connector supports the following
features:

* :ref:`sql-write-operations`:

  * :ref:`sql-data-management`, see also :ref:`hive-data-management`
  * :ref:`sql-schema-table-management`
  * :ref:`sql-views-management`

* :ref:`sql-security-operations`, see also :ref:`hive-sql-standard-based-authorization`
* :ref:`sql-transactions`

Refer to :doc:`the migration guide </appendix/from-hive>` for practical advice on migrating from Hive to Trino.

.. include:: alter-mv-set-properties-unsupported.fragment

.. _hive-alter-table-execute:

ALTER TABLE EXECUTE
^^^^^^^^^^^^^^^^^^^

The connector supports the following commands for use with
:ref:`ALTER TABLE EXECUTE <alter-table-execute>`.

optimize
~~~~~~~~

The ``optimize`` command is used for rewriting the content
of the specified non-transactional table so that it is merged
into fewer but larger files.
In case that the table is partitioned, the data compaction
acts separately on each partition selected for optimization.
This operation improves read performance.

All files with a size below the optional ``file_size_threshold``
parameter (default value for the threshold is ``100MB``) are
merged:

.. code-block:: sql

    ALTER TABLE test_table EXECUTE optimize

The following statement merges files in a table that are
under 10 megabytes in size:

.. code-block:: sql

    ALTER TABLE test_table EXECUTE optimize(file_size_threshold => '10MB')

You can use a ``WHERE`` clause with the columns used to partition the table,
to filter which partitions are optimized:

.. code-block:: sql

    ALTER TABLE test_partitioned_table EXECUTE optimize
    WHERE partition_key = 1

The ``optimize`` command is disabled by default, and can be enabled for a
catalog with the ``<catalog-name>.non_transactional_optimize_enabled``
session property:

.. code-block:: sql

    SET SESSION <catalog_name>.non_transactional_optimize_enabled=true

.. warning::

  Because Hive tables are non-transactional, take note of the following possible
  outcomes:

  * If queries are run against tables that are currently being optimized,
    duplicate rows may be read.
  * In rare cases where exceptions occur during the ``optimize`` operation,
    a manual cleanup of the table directory is needed. In this situation, refer
    to the Trino logs and query failure messages to see which files need to be
    deleted.

.. _hive-data-management:

Data management
^^^^^^^^^^^^^^^

The :ref:`sql-data-management` functionality includes support for ``INSERT``,
``UPDATE``, and ``DELETE`` statements, with the exact support depending on the
storage system, file format, and metastore:

When connecting to a Hive metastore version 3.x, the Hive connector supports
reading from and writing to insert-only and ACID tables, with full support for
partitioning and bucketing.

:doc:`/sql/delete` applied to non-transactional tables is only supported if the
table is partitioned and the ``WHERE`` clause matches entire partitions.
Transactional Hive tables with ORC format support "row-by-row" deletion, in
which the ``WHERE`` clause may match arbitrary sets of rows.

:doc:`/sql/update` is only supported for transactional Hive tables with format
ORC. ``UPDATE`` of partition or bucket columns is not supported.

ACID tables created with `Hive Streaming Ingest <https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest>`_
are not supported.

Table statistics
----------------

The Hive connector supports collecting and managing :doc:`table statistics
</optimizer/statistics>` to improve query processing performance.

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

    CALL system.drop_stats('schema_name', 'table_name')

You can also drop statistics for selected partitions only::

    CALL system.drop_stats(
        schema_name => 'schema',
        table_name => 'table',
        partition_values => ARRAY[ARRAY['p2_value1', 'p2_value2']])

.. _hive_dynamic_filtering:

Dynamic filtering
-----------------

The Hive connector supports the :doc:`dynamic filtering </admin/dynamic-filtering>` optimization.
Dynamic partition pruning is supported for partitioned tables stored in any file format
for broadcast as well as partitioned joins.
Dynamic bucket pruning is supported for bucketed tables stored in any file format for
broadcast joins only.

For tables stored in ORC or Parquet file format, dynamic filters are also pushed into
local table scan on worker nodes for broadcast joins. Dynamic filter predicates
pushed into the ORC and Parquet readers are used to perform stripe or row-group pruning
and save on disk I/O. Sorting the data within ORC or Parquet files by the columns used in
join criteria significantly improves the effectiveness of stripe or row-group pruning.
This is because grouping similar data within the same stripe or row-group
greatly improves the selectivity of the min/max indexes maintained at stripe or
row-group level.

Delaying execution for dynamic filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It can often be beneficial to wait for the collection of dynamic filters before starting
a table scan. This extra wait time can potentially result in significant overall savings
in query and CPU time, if dynamic filtering is able to reduce the amount of scanned data.

For the Hive connector, a table scan can be delayed for a configured amount of
time until the collection of dynamic filters by using the configuration property
``hive.dynamic-filtering.wait-timeout`` in the catalog file or the catalog
session property ``<hive-catalog>.dynamic_filtering_wait_timeout``.

Schema evolution
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

.. _hive_avro_schema:

Avro schema evolution
---------------------

Trino supports querying and manipulating Hive tables with the Avro storage
format, which has the schema set based on an Avro schema file/literal. Trino is
also capable of creating the tables in Trino by infering the schema from a
valid Avro schema file located locally, or remotely in HDFS/Web server.

To specify that the Avro schema should be used for interpreting table's data one must use ``avro_schema_url`` table property.
The schema can be placed remotely in
HDFS (e.g. ``avro_schema_url = 'hdfs://user/avro/schema/avro_data.avsc'``),
S3 (e.g. ``avro_schema_url = 's3n:///schema_bucket/schema/avro_data.avsc'``),
a web server (e.g. ``avro_schema_url = 'http://example.org/schema/avro_data.avsc'``)
as well as local file system. This URL, where the schema is located, must be accessible from the
Hive metastore and Trino coordinator/worker nodes.

The table created in Trino using ``avro_schema_url`` behaves the same way as a Hive table with ``avro.schema.url`` or ``avro.schema.literal`` set.

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
accessed so the query result reflects any changes in schema. Thus Trino takes advantage of Avro's backward compatibility abilities.

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
* Bucketing(``bucketed_by``) columns are not supported in ``CREATE TABLE``.
* ``ALTER TABLE`` commands modifying columns are not supported.

.. _hive-procedures:

Procedures
----------

* ``system.create_empty_partition(schema_name, table_name, partition_columns, partition_values)``

  Create an empty partition in the specified table.

* ``system.sync_partition_metadata(schema_name, table_name, mode, case_sensitive)``

  Check and update partitions list in metastore. There are three modes available:

  * ``ADD`` : add any partitions that exist on the file system, but not in the metastore.
  * ``DROP``: drop any partitions that exist in the metastore, but not on the file system.
  * ``FULL``: perform both ``ADD`` and ``DROP``.

  The ``case_sensitive`` argument is optional. The default value is ``true`` for compatibility
  with Hive's ``MSCK REPAIR TABLE`` behavior, which expects the partition column names in
  file system paths to use lowercase (e.g. ``col_x=SomeValue``). Partitions on the file system
  not conforming to this convention are ignored, unless the argument is set to ``false``.

* ``system.drop_stats(schema_name, table_name, partition_values)``

  Drops statistics for a subset of partitions or the entire table. The partitions are specified as an
  array whose elements are arrays of partition values (similar to the ``partition_values`` argument in
  ``create_empty_partition``). If ``partition_values`` argument is omitted, stats are dropped for the
  entire table.

.. _register_partition:

* ``system.register_partition(schema_name, table_name, partition_columns, partition_values, location)``

  Registers existing location as a new partition in the metastore for the specified table.

  When the ``location`` argument is omitted, the partition location is
  constructed using ``partition_columns`` and ``partition_values``.

  Due to security reasons, the procedure is enabled only when ``hive.allow-register-partition-procedure``
  is set to ``true``.

.. _unregister_partition:

* ``system.unregister_partition(schema_name, table_name, partition_columns, partition_values)``

  Unregisters given, existing partition in the metastore for the specified table.
  The partition data is not deleted.

.. _hive_flush_metadata_cache:

* ``system.flush_metadata_cache()``

  Flush all Hive metadata caches.

* ``system.flush_metadata_cache(schema_name => ..., table_name => ..., partition_columns => ARRAY[...], partition_values => ARRAY[...])``

  Flush Hive metadata cache entries connected with selected partition.
  Procedure requires named parameters to be passed

.. _hive_table_properties:

Table properties
----------------

Table properties supply or set metadata for the underlying tables. This
is key for :doc:`/sql/create-table-as` statements. Table properties are passed
to the connector using a :doc:`WITH </sql/create-table-as>` clause::

  CREATE TABLE tablename
  WITH (format='CSV',
        csv_escape = '"')

See the :ref:`hive_examples` for more information.

.. list-table:: Hive connector table properties
  :widths: 20, 60, 20
  :header-rows: 1

  * - Property name
    - Description
    - Default
  * - ``auto_purge``
    - Indicates to the configured metastore to perform a purge when a table or
      partition is deleted instead of a soft deletion using the trash.
    -
  * - ``avro_schema_url``
    - The URI pointing to :ref:`hive_avro_schema` for the table.
    -
  * - ``bucket_count``
    - The number of buckets to group data into. Only valid if used with
      ``bucketed_by``.
    - 0
  * - ``bucketed_by``
    - The bucketing column for the storage table. Only valid if used with
      ``bucket_count``.
    - ``[]``
  * - ``bucketing_version``
    - Specifies which Hive bucketing version to use. Valid values are ``1``
      or ``2``.
    -
  * - ``csv_escape``
    - The CSV escape character. Requires CSV format.
    -
  * - ``csv_quote``
    - The CSV quote character. Requires CSV format.
    -
  * - ``csv_separator``
    - The CSV separator character. Requires CSV format.
    -
  * - ``external_location``
    - The URI for an external Hive table on S3, Azure Blob Storage, etc. See the
      :ref:`hive_examples` for more information.
    -
  * - ``format``
    - The table file format. Valid values include ``ORC``, ``PARQUET``, ``AVRO``,
      ``RCBINARY``, ``RCTEXT``, ``SEQUENCEFILE``, ``JSON``, ``TEXTFILE``, and
      ``CSV``. The catalog property ``hive.storage-format`` sets the default
      value and can change it to a different default.
    -
  * - ``null_format``
    - The serialization format for ``NULL`` value. Requires TextFile, RCText,
      or SequenceFile format.
    -
  * - ``orc_bloom_filter_columns``
    - Comma separated list of columns to use for ORC bloom filter. It improves
      the performance of queries using range predicates when reading ORC files.
      Requires ORC format.
    - ``[]``
  * - ``orc_bloom_filter_fpp``
    - The ORC bloom filters false positive probability. Requires ORC format.
    - 0.05
  * - ``partitioned_by``
    - The partitioning column for the storage table. The columns listed in the
      ``partitioned_by`` clause must be the last columns as defined in the DDL.
    - ``[]``
  * - ``skip_footer_line_count``
    - The number of footer lines to ignore when parsing the file for data.
      Requires TextFile or CSV format tables.
    -
  * - ``skip_header_line_count``
    - The number of header lines to ignore when parsing the file for data.
      Requires TextFile or CSV format tables.
    -
  * - ``sorted_by``
    - The column to sort by to determine bucketing for row. Only valid if
      ``bucketed_by`` and ``bucket_count`` are specified as well.
    - ``[]``
  * - ``textfile_field_separator``
    - Allows the use of custom field separators, such as '|', for TextFile
      formatted tables.
    -
  * - ``textfile_field_separator_escape``
    - Allows the use of a custom escape character for TextFile formatted tables.
    -
  * - ``transactional``
    - Set this property to ``true`` to create an ORC ACID transactional table.
      Requires ORC format. This property may be shown as true for insert-only
      tables created using older versions of Hive.
    -

.. _hive_special_columns:

Special columns
---------------

In addition to the defined columns, the Hive connector automatically exposes
metadata in a number of hidden columns in each table:

* ``$bucket``: Bucket number for this row

* ``$path``: Full file system path name of the file for this row

* ``$file_modified_time``: Date and time of the last modification of the file for this row

* ``$file_size``: Size of the file for this row

* ``$partition``: Partition name for this row

You can use these columns in your SQL statements like any other column. They
can be selected directly, or used in conditional statements. For example, you
can inspect the file size, location and partition for each record::

    SELECT *, "$path", "$file_size", "$partition"
    FROM hive.web.page_views;

Retrieve all records that belong to files stored in the partition
``ds=2016-08-09/country=US``::

    SELECT *, "$path", "$file_size"
    FROM hive.web.page_views
    WHERE "$partition" = 'ds=2016-08-09/country=US'

.. _hive_special_tables:

Special tables
--------------

The raw Hive table properties are available as a hidden table, containing a
separate column per table property, with a single row containing the property
values. The properties table name is the same as the table name with
``$properties`` appended.

You can inspect the property names and values with a simple query::

    SELECT * FROM hive.web."page_views$properties";

.. _hive_examples:

Examples
--------

The Hive connector supports querying and manipulating Hive tables and schemas
(databases). While some uncommon operations need to be performed using
Hive directly, most operations can be performed using Trino.

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

Hive 3 related limitations
--------------------------

* For security reasons, the ``sys`` system catalog is not accessible.

* Hive's ``timestamp with local zone`` data type is not supported.
  It is possible to read from a table with a column of this type, but the column
  data is not accessible. Writing to such a table is not supported.

* Due to Hive issues `HIVE-21002 <https://issues.apache.org/jira/browse/HIVE-21002>`_
  and `HIVE-22167 <https://issues.apache.org/jira/browse/HIVE-22167>`_, Trino does
  not correctly read ``timestamp`` values from Parquet, RCBinary, or Avro
  file formats created by Hive 3.1 or later. When reading from these file formats,
  Trino returns different results than Hive.

* :doc:`/sql/create-table-as` can be used to create transactional tables in ORC format like this::

      CREATE TABLE <name>
      WITH (
          format='ORC',
          transactional=true
      )
      AS <query>

  Trino does not support gathering table statistics for Hive transactional tables.
  You need to use Hive to gather table statistics with ``ANALYZE TABLE COMPUTE STATISTICS`` after table creation.
