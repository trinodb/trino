==============
Hive Connector
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
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database, such as MySQL, and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
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
and :doc:`Azure Storage <hive-azure>`.

The coordinator and all workers must have network access to the Hive metastore
and the storage system.

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

Metastore Configuration for Avro
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

.. _hive_configuration_properties:

Hive Configuration Properties
-----------------------------

================================================== ============================================================ ============
Property Name                                      Description                                                  Default
================================================== ============================================================ ============
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

``hive.file-status-cache-tables``                  Cache directory listing for specific tables. Examples:

                                                   * ``fruit.apple,fruit.orange`` to cache listings only for
                                                     tables ``apple`` and ``orange`` in schema ``fruit``
                                                   * ``fruit.*,vegetable.*`` to cache listings for all tables
                                                     in schemas ``fruit`` and ``vegetable``
                                                   * ``*`` to cache listings for all tables in all schemas

``hive.file-status-cache-size``                    Maximum total number of cached file status entries.          1,000,000

``hive.file-status-cache-expire-time``             How long a cached directory listing should be considered     ``1m``
                                                   valid.

``hive.parquet.time-zone``                         Adjusts timestamp values to a specific time zone.     	JVM default
                                                   For Hive 3.1+, this should be set to UTC.

``hive.rcfile.time-zone``                          Adjusts binary encoded timestamp values to a specific	JVM default
                                                   time zone. For Hive 3.1+, this should be set to UTC.

``hive.orc.time-zone``                             Sets the default time zone for legacy ORC files that did	JVM default
                                                   not declare a time zone.

``hive.timestamp-precision``                       Specifies the precision to use for columns of type 	        ``MILLISECONDS``
                                                   ``timestamp``. Possible values are ``MILLISECONDS``,
                                                   ``MICROSECONDS`` and ``NANOSECONDS``. Write operations
                                                   are only supported for ``MILLISECONDS``.

``hive.temporary-staging-directory-enabled``       Controls whether the temporary staging directory configured  ``true``
                                                   at ``hive.temporary-staging-directory-path`` should be
                                                   used for write operations. Temporary staging directory is
                                                   never used for writes to non-sorted tables on S3,
                                                   encrypted HDFS or external location. Writes to sorted tables
                                                   will utilize this path for staging temporary files
                                                   during sorting operation. When disabled, the target storage
                                                   will be used for staging while writing sorted tables which
                                                   can be inefficient when writing to object stores like S3.

``hive.temporary-staging-directory-path``          Controls the location of temporary staging directory that    ``/tmp/${USER}``
                                                   is used for write operations. The ``${USER}`` placeholder
                                                   can be used to use a different location for each user.
================================================== ============================================================ ============

Metastore Configuration Properties
----------------------------------

The required Hive metastore can be configured with a number of properties.
Specific properties can be used to further configure the
`Thrift <#thrift-metastore-configuration-properties>`__ or
`Glue <#aws-glue-catalog-configuration-properties>`__ metastore.

======================================= ============================================================ ============
Property Name                                      Description                                       Default
======================================= ============================================================ ============
``hive.metastore``                      The type of Hive metastore to use. Presto currently supports ``thrift``
                                        the default Hive Thrift metastore (``thrift``), and the AWS
                                        Glue Catalog (``glue``) as metadata sources.

``hive.metastore-cache-ttl``            Duration how long cached metastore data should be considered ``0s``
                                        valid.

``hive.metastore-cache-maximum-size``   Hive metastore cache maximum size.                            10000

``hive.metastore-refresh-interval``     Asynchronously refresh cached metastore data after access
                                        if it is older than this but is not yet expired, allowing
                                        subsequent accesses to see fresh data.

``hive.metastore-refresh-max-threads``  Maximum threads used to refresh cached metastore data.        100

``hive.metastore-timeout``              Timeout for Hive metastore requests.                         ``10s``
======================================= ============================================================ ============

Thrift Metastore Configuration Properties
-----------------------------------------

=============================================================== ============================================================ ============
Property Name                                                   Description                                                  Default
=============================================================== ============================================================ ============
``hive.metastore.uri``                                          The URI(s) of the Hive metastore to connect to using the
                                                                Thrift protocol. If multiple URIs are provided, the first
                                                                URI is used by default, and the rest of the URIs are
                                                                fallback metastores. This property is required.
                                                                Example: ``thrift://192.0.2.3:9083`` or
                                                                ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.metastore.username``                                     The username Presto uses to access the Hive metastore.

``hive.metastore.authentication.type``                          Hive metastore authentication type.
                                                                Possible values are ``NONE`` or ``KERBEROS``
                                                                (defaults to ``NONE``).

``hive.metastore.thrift.impersonation.enabled``                 Enable Hive metastore end user impersonation.

``hive.metastore.thrift.delegation-token.cache-ttl``            Time to live delegation token cache for metastore.           ``1h``

``hive.metastore.thrift.delegation-token.cache-maximum-size``   Delegation token cache maximum size.                         1,000

``hive.metastore.thrift.client.ssl.enabled``                    Use SSL when connecting to metastore.                        ``false``

``hive.metastore.thrift.client.ssl.key``                        Path to private key and client certificate (key store).

``hive.metastore.thrift.client.ssl.key-password``               Password for the private key.

``hive.metastore.thrift.client.ssl.trust-certificate``          Path to the server certificate chain (trust store).
                                                                Required when SSL is enabled.

``hive.metastore.thrift.client.ssl.trust-certificate-password`` Password for the trust store

``hive.metastore.service.principal``                            The Kerberos principal of the Hive metastore service.

``hive.metastore.client.principal``                             The Kerberos principal that Presto uses when connecting
                                                                to the Hive metastore service.

``hive.metastore.client.keytab``                                Hive metastore client keytab location.

=============================================================== ============================================================ ============

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

``hive.metastore.glue.endpoint-url``                 Glue API endpoint URL (optional).
                                                     Example: ``https://glue.us-east-1.amazonaws.com``

``hive.metastore.glue.pin-client-to-current-region`` Pin Glue requests to the same region as the EC2 instance
                                                     where Presto is running, defaults to ``false``.

``hive.metastore.glue.max-connections``              Max number of concurrent connections to Glue,
                                                     defaults to ``5``.

``hive.metastore.glue.max-error-retries``            Maximum number of error retries for the Glue client,
                                                     defaults to ``10``.

``hive.metastore.glue.default-warehouse-dir``        Default warehouse directory for schemas created without an
                                                     explicit ``location`` property.

``hive.metastore.glue.aws-credentials-provider``     Fully qualified name of the Java class to use for obtaining
                                                     AWS credentials. Can be used to supply a custom credentials
                                                     provider.

``hive.metastore.glue.aws-access-key``               AWS access key to use to connect to the Glue Catalog. If
                                                     specified along with ``hive.metastore.glue.aws-secret-key``,
                                                     this parameter takes precedence over
                                                     ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.aws-secret-key``               AWS secret key to use to connect to the Glue Catalog. If
                                                     specified along with ``hive.metastore.glue.aws-access-key``,
                                                     this parameter takes precedence over
                                                     ``hive.metastore.glue.iam-role``.

``hive.metastore.glue.catalogid``                    The ID of the Glue Catalog in which the metadata database
                                                     resides.

``hive.metastore.glue.iam-role``                     ARN of an IAM role to assume when connecting to the Glue
                                                     Catalog.

``hive.metastore.glue.external-id``                  External ID for the IAM role trust policy when connecting
                                                     to the Glue Catalog.

``hive.metastore.glue.partitions-segments``          Number of segments for partitioned Glue tables, defaults
                                                     to ``5``.

``hive.metastore.glue.get-partition-threads``        Number of threads for parallel partition fetches from Glue,
                                                     defaults to ``20``.
==================================================== ============================================================

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

    CALL system.drop_stats(schema_name, table_name)

You can also drop statistics for selected partitions only::

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

Special Columns
---------------

In addition to the defined columns, the Hive connector automatically exposes
metadata in a number of hidden columns in each table. You can use these columns
in your SQL statements like any other column, e.g., they can be selected
directly or used in conditional statements.

* ``$bucket``: Bucket number for this row

* ``$path``: Full file system path name of the file for this row

* ``$file_modified_time``: Date and time of the last modification of the file for this row

* ``$file_size``: Size of the file for this row

* ``$partition``: Partition name for this row

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

* :doc:`/sql/create-table-as` can be used to create transactional tables in ORC format like this::

      CREATE TABLE <name>
      WITH (
          format='ORC',
          transactional=true,
      )
      AS <query>

  Presto does not support gathering table statistics for Hive transactional tables.
  You need to use Hive to gather table statistics with ``ANALYZE TABLE COMPUTE STATISTICS`` after table creation.
