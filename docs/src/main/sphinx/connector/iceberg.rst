=================
Iceberg connector
=================

.. raw:: html

  <img src="../_static/img/iceberg.png" class="connector-logo">

Apache Iceberg is an open table format for huge analytic datasets.
The Iceberg connector allows querying data stored in
files written in Iceberg format, as defined in the
`Iceberg Table Spec <https://iceberg.apache.org/spec/>`_. It supports Apache
Iceberg table spec version 1 and 2.

The Iceberg table state is maintained in metadata files. All changes to table state
create a new metadata file and replace the old metadata with an atomic swap.
The table metadata file tracks the table schema, partitioning config,
custom properties, and snapshots of the table contents.

Iceberg data files can be stored in either Parquet, ORC or Avro format, as
determined by the ``format`` property in the table definition.  The
table ``format`` defaults to ``ORC``.

Iceberg is designed to improve on the known scalability limitations of Hive, which stores
table metadata in a metastore that is backed by a relational database such as MySQL.  It tracks
partition locations in the metastore, but not individual data files.  Trino queries
using the :doc:`/connector/hive` must first call the metastore to get partition locations,
then call the underlying filesystem to list all data files inside each partition,
and then read metadata from each data file.

Since Iceberg stores the paths to data files in the metadata files, it
only consults the underlying file system for files that must be read.

Requirements
------------

To use Iceberg, you need:

* Network access from the Trino coordinator and workers to the distributed
  object storage.
* Access to a Hive metastore service (HMS) or AWS Glue.
* Network access from the Trino coordinator to the HMS. Hive
  metastore access with the Thrift protocol defaults to using port 9083.

Configuration
-------------

The connector supports multiple Iceberg catalog types, you may use either a Hive
metastore service (HMS), AWS Glue, or a REST catalog. The catalog type is determined by the
``iceberg.catalog.type`` property, it can be set to ``HIVE_METASTORE``, ``GLUE``, ``JDBC``, or ``REST``.


.. _iceberg-hive-catalog:

Hive metastore catalog
^^^^^^^^^^^^^^^^^^^^^^

The Hive metastore catalog is the default implementation.
When using it, the Iceberg connector supports the same metastore
configuration properties as the Hive connector. At a minimum,
``hive.metastore.uri`` must be configured, see
:ref:`Thrift metastore configuration<hive-thrift-metastore>`.

.. code-block:: text

    connector.name=iceberg
    hive.metastore.uri=thrift://localhost:9083

.. _iceberg-glue-catalog:

Glue catalog
^^^^^^^^^^^^

When using the Glue catalog, the Iceberg connector supports the same
configuration properties as the Hive connector's Glue setup. See
:ref:`AWS Glue metastore configuration<hive-glue-metastore>`.

.. code-block:: text

    connector.name=iceberg
    iceberg.catalog.type=glue

.. _iceberg-rest-catalog:

REST catalog
^^^^^^^^^^^^^^

In order to use the Iceberg REST catalog, ensure to configure the catalog type with
``iceberg.catalog.type=rest`` and provide further details with the following
properties:

============================================== ============================================================
Property Name                                        Description
============================================== ============================================================
``iceberg.rest-catalog.uri``                   REST server API endpoint URI (required).
                                               Example: ``http://iceberg-with-rest:8181``

``iceberg.rest-catalog.warehouse``             Warehouse identifier/location for the catalog (optional).
                                               Example: ``s3://my_bucket/warehouse_location``

``iceberg.rest-catalog.security``              The type of security to use (default: ``NONE``).  ``OAUTH2``
                                               requires either a ``token`` or ``credential``.
                                               Example: ``OAUTH2``

``iceberg.rest-catalog.session``               Session information included when communicating with the REST Catalog.
                                               Options are ``NONE`` or ``USER`` (default: ``NONE``).

``iceberg.rest-catalog.oauth2.token``          The Bearer token which will be used for interactions
                                               with the server. A ``token`` or ``credential`` is required for
                                               ``OAUTH2`` security.
                                               Example: ``AbCdEf123456``

``iceberg.rest-catalog.oauth2.credential``     The credential to exchange for a token in the OAuth2 client
                                               credentials flow with the server. A ``token`` or ``credential``
                                               is required for ``OAUTH2`` security.
                                               Example: ``AbCdEf123456``
============================================== ============================================================

.. code-block:: text

    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://iceberg-with-rest:8181

REST catalog does not support :doc:`views</sql/create-view>` or
:doc:`materialized views</sql/create-materialized-view>`.

.. _iceberg-jdbc-catalog:

JDBC catalog
^^^^^^^^^^^^

.. warning::

  The JDBC catalog may face the compatibility issue if Iceberg introduces breaking changes in the future.
  Consider the :ref:`REST catalog <iceberg-rest-catalog>` as an alternative solution.

At a minimum, ``iceberg.jdbc-catalog.driver-class``, ``iceberg.jdbc-catalog.connection-url`` and
``iceberg.jdbc-catalog.catalog-name`` must be configured.
When using any database besides PostgreSQL, a JDBC driver jar file must be placed in the plugin directory.

.. code-block:: text

    connector.name=iceberg
    iceberg.catalog.type=jdbc
    iceberg.jdbc-catalog.catalog-name=test
    iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
    iceberg.jdbc-catalog.connection-url=jdbc:postgresql://example.net:5432/database
    iceberg.jdbc-catalog.connection-user=admin
    iceberg.jdbc-catalog.connection-password=test
    iceberg.jdbc-catalog.default-warehouse-dir=s3://bucket

JDBC catalog does not support :doc:`views</sql/create-view>` or
:doc:`materialized views</sql/create-materialized-view>`.

General configuration
^^^^^^^^^^^^^^^^^^^^^

These configuration properties are independent of which catalog implementation
is used.

.. list-table:: Iceberg general configuration properties
  :widths: 30, 58, 12
  :header-rows: 1

  * - Property name
    - Description
    - Default
  * - ``iceberg.file-format``
    - Define the data storage file format for Iceberg tables.
      Possible values are

      * ``PARQUET``
      * ``ORC``
      * ``AVRO``
    - ``ORC``
  * - ``iceberg.compression-codec``
    - The compression codec to be used when writing files.
      Possible values are

      * ``NONE``
      * ``SNAPPY``
      * ``LZ4``
      * ``ZSTD``
      * ``GZIP``
    - ``ZSTD``
  * - ``iceberg.use-file-size-from-metadata``
    - Read file sizes from metadata instead of file system.
      This property should only be set as a workaround for
      `this issue <https://github.com/apache/iceberg/issues/1980>`_.
      The problem was fixed in Iceberg version 0.11.0.
    - ``true``
  * - ``iceberg.max-partitions-per-writer``
    - Maximum number of partitions handled per writer.
    - 100
  * - ``iceberg.target-max-file-size``
    - Target maximum size of written files; the actual size may be larger.
    - ``1GB``
  * - ``iceberg.unique-table-location``
    - Use randomized, unique table locations.
    - ``true``
  * - ``iceberg.dynamic-filtering.wait-timeout``
    - Maximum duration to wait for completion of dynamic filters during split generation.
    - ``0s``
  * - ``iceberg.delete-schema-locations-fallback``
    - Whether schema locations should be deleted when Trino can't determine whether they contain external files.
    - ``false``
  * - ``iceberg.minimum-assigned-split-weight``
    - A decimal value in the range (0, 1] used as a minimum for weights assigned to each split. A low value may improve performance
      on tables with small files. A higher value may improve performance for queries with highly skewed aggregations or joins.
    - 0.05
  * - ``iceberg.table-statistics-enabled``
    - Enables :doc:`/optimizer/statistics`. The equivalent
      :doc:`catalog session property </sql/set-session>`
      is ``statistics_enabled`` for session specific use.
      Set to ``false`` to disable statistics. Disabling statistics
      means that :doc:`/optimizer/cost-based-optimizations` can
      not make smart decisions about the query plan.
    - ``true``
  * - ``iceberg.projection-pushdown-enabled``
    - Enable :doc:`projection pushdown </optimizer/pushdown>`
    - ``true``
  * - ``iceberg.hive-catalog-name``
    - Catalog to redirect to when a Hive table is referenced.
    -
  * - ``iceberg.materialized-views.storage-schema``
    - Schema for creating materialized views storage tables. When this property
      is not configured, storage tables are created in the same schema as the
      materialized view definition. When the ``storage_schema`` materialized
      view property is specified, it takes precedence over this catalog property.
    - Empty
  * - ``iceberg.register-table-procedure.enabled``
    - Enable to allow user to call ``register_table`` procedure
    - ``false``

ORC format configuration
^^^^^^^^^^^^^^^^^^^^^^^^

The following properties are used to configure the read and write operations
with ORC files performed by the Iceberg connector.

.. list-table:: ORC format configuration properties
  :widths: 30, 58, 12
  :header-rows: 1

  * - Property name
    - Description
    - Default
  * - ``hive.orc.bloom-filters.enabled``
    - Enable bloom filters for predicate pushdown.
    - ``false``

Parquet format configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following properties are used to configure the read and write operations
with Parquet files performed by the Iceberg connector.

.. list-table:: Parquet format configuration properties
    :widths: 30, 50, 20
    :header-rows: 1

    * - Property Name
      - Description
      - Default
    * - ``parquet.max-read-block-row-count``
      - Sets the maximum number of rows read in a batch.
      - ``8192``
    * - ``parquet.optimized-reader.enabled``
      - Whether batched column readers should be used when reading Parquet files
        for improved performance. Set this property to ``false`` to disable the
        optimized parquet reader by default. The equivalent catalog session
        property is ``parquet_optimized_reader_enabled``.
      - ``true``
    * - ``parquet.optimized-nested-reader.enabled``
      - Whether batched column readers should be used when reading ARRAY, MAP
        and ROW types from Parquet files for improved performance. Set this
        property to ``false`` to disable the optimized parquet reader by default
        for structural data types. The equivalent catalog session property is
        ``parquet_optimized_nested_reader_enabled``.
      - ``true``

.. _iceberg-authorization:

Authorization checks
^^^^^^^^^^^^^^^^^^^^

You can enable authorization checks for the connector by setting
the ``iceberg.security`` property in the catalog properties file. This
property must be one of the following values:

.. list-table:: Iceberg security values
  :widths: 30, 60
  :header-rows: 1

  * - Property value
    - Description
  * - ``ALLOW_ALL``
    - No authorization checks are enforced.
  * - ``SYSTEM``
    - The connector relies on system-level access control.
  * - ``READ_ONLY``
    - Operations that read data or metadata, such as :doc:`/sql/select` are
      permitted. No operations that write data or metadata, such as
      :doc:`/sql/create-table`, :doc:`/sql/insert`, or :doc:`/sql/delete` are
      allowed.
  * - ``FILE``
    - Authorization checks are enforced using a catalog-level access control
      configuration file whose path is specified in the ``security.config-file``
      catalog configuration property. See
      :ref:`catalog-file-based-access-control` for information on the
      authorization configuration file.

.. _iceberg-table-redirection:

Table redirection
-----------------

.. include:: table-redirection.fragment

The connector supports redirection from Iceberg tables to Hive tables
with the ``iceberg.hive-catalog-name`` catalog configuration property.

.. _iceberg-sql-support:

SQL support
-----------

This connector provides read access and write access to data and metadata in
Iceberg. In addition to the :ref:`globally available <sql-globally-available>`
and :ref:`read operation <sql-read-operations>` statements, the connector
supports the following features:

* :ref:`sql-write-operations`:

  * :ref:`iceberg-schema-table-management` and :ref:`iceberg-tables`
  * :ref:`iceberg-data-management`
  * :ref:`sql-view-management`
  * :ref:`sql-materialized-view-management`, see also :ref:`iceberg-materialized-views`

.. _iceberg-schema-table-management:

Schema and table management
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :ref:`sql-schema-table-management` functionality includes support for:

* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`
* :doc:`/sql/alter-schema`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/alter-table`
* :doc:`/sql/comment`

.. _iceberg-create-schema:

CREATE SCHEMA
~~~~~~~~~~~~~

The connector supports creating schemas. You can create a schema with or without
a specified location.

You can create a schema with the :doc:`/sql/create-schema` statement and the
``location`` schema property. The tables in this schema, which have no explicit
``location`` set in :doc:`/sql/create-table` statement, are located in a
subdirectory under the directory corresponding to the schema location.

Create a schema on S3::

  CREATE SCHEMA example.example_s3_schema
  WITH (location = 's3://my-bucket/a/path/');

Create a schema on a S3 compatible object storage such as MinIO::

  CREATE SCHEMA example.example_s3a_schema
  WITH (location = 's3a://my-bucket/a/path/');

Create a schema on HDFS::

  CREATE SCHEMA example.example_hdfs_schema
  WITH (location='hdfs://hadoop-master:9000/user/hive/warehouse/a/path/');

Optionally, on HDFS, the location can be omitted::

  CREATE SCHEMA example.example_hdfs_schema;

.. _iceberg-create-table:

Creating tables
~~~~~~~~~~~~~~~

The Iceberg connector supports creating tables using the :doc:`CREATE
TABLE </sql/create-table>` syntax. Optionally specify the
:ref:`table properties <iceberg-table-properties>` supported by this connector::

    CREATE TABLE example_table (
        c1 integer,
        c2 date,
        c3 double
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['c1', 'c2'],
        sorted_by = ARRAY['c3'],
        location = 's3://my-bucket/a/path/'
    );

When the ``location`` table property is omitted, the content of the table
is stored in a subdirectory under the directory corresponding to the
schema location.

The Iceberg connector supports creating tables using the :doc:`CREATE
TABLE AS </sql/create-table-as>` with :doc:`SELECT </sql/select>` syntax::

    CREATE TABLE tiny_nation
    WITH (
        format = 'PARQUET'
    )
    AS
        SELECT *
        FROM nation
        WHERE nationkey < 10;

Another flavor of creating tables with :doc:`CREATE TABLE AS </sql/create-table-as>`
is with :doc:`VALUES </sql/values>` syntax::

    CREATE TABLE yearly_clicks (
        year,
        clicks
    )
    WITH (
        partitioning = ARRAY['year']
    )
    AS VALUES
        (2021, 10000),
        (2022, 20000);

``NOT NULL`` column constraint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Iceberg connector supports setting ``NOT NULL`` constraints on the table columns.

The ``NOT NULL`` constraint can be set on the columns, while creating tables by
using the :doc:`CREATE TABLE </sql/create-table>` syntax::

    CREATE TABLE example_table (
        year INTEGER NOT NULL,
        name VARCHAR NOT NULL,
        age INTEGER,
        address VARCHAR
    );

When trying to insert/update data in the table, the query fails if trying
to set ``NULL`` value on a column having the ``NOT NULL`` constraint.

DROP TABLE
~~~~~~~~~~

The Iceberg connector supports dropping a table by using the :doc:`/sql/drop-table`
syntax. When the command succeeds, both the data of the Iceberg table and also the
information related to the table in the metastore service are removed.
Dropping tables which have their data/metadata stored in a different location than
the table's corresponding base directory on the object store is not supported.

.. _iceberg-comment:

.. _iceberg-alter-table-execute:

ALTER TABLE EXECUTE
~~~~~~~~~~~~~~~~~~~

The connector supports the following commands for use with
:ref:`ALTER TABLE EXECUTE <alter-table-execute>`.

optimize
""""""""

The ``optimize`` command is used for rewriting the active content
of the specified table so that it is merged into fewer but
larger files.
In case that the table is partitioned, the data compaction
acts separately on each partition selected for optimization.
This operation improves read performance.

All files with a size below the optional ``file_size_threshold``
parameter (default value for the threshold is ``100MB``) are
merged:

.. code-block:: sql

    ALTER TABLE test_table EXECUTE optimize

The following statement merges the files in a table that
are under 10 megabytes in size:

.. code-block:: sql

    ALTER TABLE test_table EXECUTE optimize(file_size_threshold => '10MB')

You can use a ``WHERE`` clause with the columns used to partition
the table, to apply ``optimize`` only on the partition(s) corresponding
to the filter:

.. code-block:: sql

    ALTER TABLE test_partitioned_table EXECUTE optimize
    WHERE partition_key = 1

expire_snapshots
""""""""""""""""

The ``expire_snapshots`` command removes all snapshots and all related metadata and data files.
Regularly expiring snapshots is recommended to delete data files that are no longer needed,
and to keep the size of table metadata small.
The procedure affects all snapshots that are older than the time period configured with the ``retention_threshold`` parameter.

``expire_snapshots`` can be run as follows:

.. code-block:: sql

  ALTER TABLE test_table EXECUTE expire_snapshots(retention_threshold => '7d')

The value for ``retention_threshold`` must be higher than or equal to ``iceberg.expire_snapshots.min-retention`` in the catalog
otherwise the procedure will fail with similar message:
``Retention specified (1.00d) is shorter than the minimum retention configured in the system (7.00d)``.
The default value for this property is ``7d``.

remove_orphan_files
"""""""""""""""""""

The ``remove_orphan_files`` command removes all files from table's data directory which are
not linked from metadata files and that are older than the value of ``retention_threshold`` parameter.
Deleting orphan files from time to time is recommended to keep size of table's data directory under control.

``remove_orphan_files`` can be run as follows:

.. code-block:: sql

  ALTER TABLE test_table EXECUTE remove_orphan_files(retention_threshold => '7d')

The value for ``retention_threshold`` must be higher than or equal to ``iceberg.remove_orphan_files.min-retention`` in the catalog
otherwise the procedure will fail with similar message:
``Retention specified (1.00d) is shorter than the minimum retention configured in the system (7.00d)``.
The default value for this property is ``7d``.

.. _drop-extended-stats:

drop_extended_stats
~~~~~~~~~~~~~~~~~~~

The ``drop_extended_stats`` command removes all extended statistics information from
the table.

``drop_extended_stats`` can be run as follows:

.. code-block:: sql

  ALTER TABLE test_table EXECUTE drop_extended_stats

.. _iceberg-alter-table-set-properties:

ALTER TABLE SET PROPERTIES
~~~~~~~~~~~~~~~~~~~~~~~~~~

The connector supports modifying the properties on existing tables using
:ref:`ALTER TABLE SET PROPERTIES <alter-table-set-properties>`.

The following table properties can be updated after a table is created:

* ``format``
* ``format_version``
* ``partitioning``
* ``sorted_by``

For example, to update a table from v1 of the Iceberg specification to v2:

.. code-block:: sql

    ALTER TABLE table_name SET PROPERTIES format_version = 2;

Or to set the column ``my_new_partition_column`` as a partition column on a table:

.. code-block:: sql

    ALTER TABLE table_name SET PROPERTIES partitioning = ARRAY[<existing partition columns>, 'my_new_partition_column'];

The current values of a table's properties can be shown using :doc:`SHOW CREATE TABLE </sql/show-create-table>`.

COMMENT
~~~~~~~

The Iceberg connector supports setting comments on the following objects:

- tables
- views
- table columns

The ``COMMENT`` option is supported on both the table and
the table columns for the :doc:`/sql/create-table` operation.

The ``COMMENT`` option is supported for adding table columns
through the :doc:`/sql/alter-table` operations.

The connector supports the command :doc:`COMMENT </sql/comment>` for setting
comments on existing entities.

.. _iceberg-data-management:

Data management
^^^^^^^^^^^^^^^

The :ref:`sql-data-management` functionality includes support for ``INSERT``,
``UPDATE``, ``DELETE``, and ``MERGE`` statements.

.. _iceberg-delete:

Deletion by partition
~~~~~~~~~~~~~~~~~~~~~

For partitioned tables, the Iceberg connector supports the deletion of entire
partitions if the ``WHERE`` clause specifies filters only on the identity-transformed
partitioning columns, that can match entire partitions. Given the table definition
from :ref:`Partitioned Tables <iceberg-tables>` section,
the following SQL statement deletes all partitions for which ``country`` is ``US``::

    DELETE FROM example.testdb.customer_orders
    WHERE country = 'US'

A partition delete is performed if the ``WHERE`` clause meets these conditions.

Row level deletion
~~~~~~~~~~~~~~~~~~

Tables using v2 of the Iceberg specification support deletion of individual rows
by writing position delete files.

Type mapping
------------

The connector reads and writes data into the supported data file formats Avro,
ORC, and Parquet, following the Iceberg specification.

Because Trino and Iceberg each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

The Iceberg specification includes supported data types and the mapping to the
formating in the Avro, ORC, or Parquet files:

* `Iceberg to Avro <https://iceberg.apache.org/spec/#avro>`_
* `Iceberg to ORC <https://iceberg.apache.org/spec/#orc>`_
* `Iceberg to Parquet <https://iceberg.apache.org/spec/#parquet>`_

Iceberg to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Iceberg types to the corresponding Trino types following this
table:

.. list-table:: Iceberg to Trino type mapping
  :widths: 40, 60
  :header-rows: 1

  * - Iceberg type
    - Trino type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``INT``
    - ``INTEGER``
  * - ``LONG``
    - ``BIGINT``
  * - ``FLOAT``
    - ``REAL``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
  * - ``DATE``
    - ``DATE``
  * - ``TIME``
    - ``TIME(6)``
  * - ``TIMESTAMP``
    - ``TIMESTAMP(6)``
  * - ``TIMESTAMPTZ``
    - ``TIMESTAMP(6) WITH TIME ZONE``
  * - ``STRING``
    - ``VARCHAR``
  * - ``UUID``
    - ``UUID``
  * - ``BINARY``
    - ``VARBINARY``
  * - ``FIXED (L)``
    - ``VARBINARY``
  * - ``STRUCT(...)``
    - ``ROW(...)``
  * - ``LIST(e)``
    - ``ARRAY(e)``
  * - ``MAP(k,v)``
    - ``MAP(k,v)``

No other types are supported.

Trino to Iceberg type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Trino types to the corresponding Iceberg types following
this table:

.. list-table:: Trino to Iceberg type mapping
  :widths: 40, 60
  :header-rows: 1

  * - Trino type
    - Iceberg type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``INTEGER``
    - ``INT``
  * - ``BIGINT``
    - ``LONG``
  * - ``REAL``
    - ``FLOAT``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
  * - ``DATE``
    - ``DATE``
  * - ``TIME(6)``
    - ``TIME``
  * - ``TIMESTAMP(6)``
    - ``TIMESTAMP``
  * - ``TIMESTAMP(6) WITH TIME ZONE``
    - ``TIMESTAMPTZ``
  * - ``VARCHAR``
    - ``STRING``
  * - ``UUID``
    - ``UUID``
  * - ``VARBINARY``
    - ``BINARY``
  * - ``ROW(...)``
    - ``STRUCT(...)``
  * - ``ARRAY(e)``
    - ``LIST(e)``
  * - ``MAP(k,v)``
    - ``MAP(k,v)``

No other types are supported.

.. _iceberg-tables:

Partitioned tables
------------------

Iceberg supports partitioning by specifying transforms over the table columns.
A partition is created for each unique tuple value produced by the transforms.
Identity transforms are simply the column name. Other transforms are:

===================================== ====================================================================
Transform                             Description
===================================== ====================================================================
``year(ts)``                          A partition is created for each year.  The partition value is the
                                      integer difference in years between ``ts`` and January 1 1970.

``month(ts)``                         A partition is created for each month of each year.  The partition
                                      value is the integer difference in months between ``ts`` and
                                      January 1 1970.

``day(ts)``                           A partition is created for each day of each year.  The partition
                                      value is the integer difference in days between ``ts`` and
                                      January 1 1970.

``hour(ts)``                          A partition is created hour of each day.  The partition value
                                      is a timestamp with the minutes and seconds set to zero.

``bucket(x, nbuckets)``               The data is hashed into the specified number of buckets.  The
                                      partition value is an integer hash of ``x``, with a value between
                                      0 and ``nbuckets - 1`` inclusive.

``truncate(s, nchars)``               The partition value is the first ``nchars`` characters of ``s``.
===================================== ====================================================================

In this example, the table is partitioned by the month of ``order_date``, a hash of
``account_number`` (with 10 buckets), and ``country``::

    CREATE TABLE example.testdb.customer_orders (
        order_id BIGINT,
        order_date DATE,
        account_number BIGINT,
        customer VARCHAR,
        country VARCHAR)
    WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country'])

Sorted tables
-------------

The connector supports sorted files as a performance improvement. Data is
sorted during writes within each file based on the specified array of one
or more columns.

Sorting is particularly beneficial when the sorted columns show a
high cardinality and are used as a filter for selective reads.

The sort order is configured with the ``sorted_by`` table property.
Specify an array of one or more columns to use for sorting
when creating the table. The following example configures the
``order_date`` column of the ``orders`` table in the ``customers``
schema in the ``example`` catalog::

    CREATE TABLE example.customers.orders (
        order_id BIGINT,
        order_date DATE,
        account_number BIGINT,
        customer VARCHAR,
        country VARCHAR)
    WITH (sorted_by = ARRAY['order_date'])

Sorting can be combined with partitioning on the same column. For example::

    CREATE TABLE example.customers.orders (
        order_id BIGINT,
        order_date DATE,
        account_number BIGINT,
        customer VARCHAR,
        country VARCHAR)
    WITH (
        partitioning = ARRAY['month(order_date)'],
        sorted_by = ARRAY['order_date']
    )

You can disable sorted writing with the session property
``sorted_writing_enabled`` set to ``false``.

Rolling back to a previous snapshot
-----------------------------------

Iceberg supports a "snapshot" model of data, where table snapshots are
identified by a snapshot ID.

The connector provides a system table exposing snapshot information for every
Iceberg table. Snapshots are identified by ``BIGINT`` snapshot IDs.
For example, you could find the snapshot IDs for the ``customer_orders`` table
by running the following query::

    SELECT snapshot_id
    FROM example.testdb."customer_orders$snapshots"
    ORDER BY committed_at DESC

.. _iceberg-time-travel:

Time travel queries
^^^^^^^^^^^^^^^^^^^

The connector offers the ability to query historical data.
This allows you to query the table as it was when a previous snapshot
of the table was taken, even if the data has since been modified or deleted.

The historical data of the table can be retrieved by specifying the
snapshot identifier corresponding to the version of the table that
needs to be retrieved::

   SELECT *
   FROM example.testdb.customer_orders FOR VERSION AS OF 8954597067493422955

A different approach of retrieving historical data is to specify
a point in time in the past, such as a day or week ago. The latest snapshot
of the table taken before or at the specified timestamp in the query is
internally used for providing the previous state of the table::

   SELECT *
   FROM example.testdb.customer_orders FOR TIMESTAMP AS OF TIMESTAMP '2022-03-23 09:59:29.803 Europe/Vienna'

Rolling back to a previous snapshot
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``$snapshots`` metadata table to determine the latest snapshot ID of the table like in the following query::

    SELECT snapshot_id
    FROM example.testdb."customer_orders$snapshots"
    ORDER BY committed_at DESC LIMIT 1

The procedure ``system.rollback_to_snapshot`` allows the caller to roll back
the state of the table to a previous snapshot id::

    CALL example.system.rollback_to_snapshot('testdb', 'customer_orders', 8954597067493422955)

Schema evolution
----------------

Iceberg supports schema evolution, with safe column add, drop, reorder
and rename operations, including in nested structures.
Table partitioning can also be changed and the connector can still
query data created before the partitioning change.

.. _iceberg-register-table:

Register table
--------------
The connector can register existing Iceberg tables with the catalog.

The procedure ``system.register_table`` allows the caller to register an
existing Iceberg table in the metastore, using its existing metadata and data
files::

    CALL example.system.register_table(schema_name => 'testdb', table_name => 'customer_orders', table_location => 'hdfs://hadoop-master:9000/user/hive/warehouse/customer_orders-581fad8517934af6be1857a903559d44')

In addition, you can provide a file name to register a table
with specific metadata. This may be used to register the table with
some specific table state, or may be necessary if the connector cannot
automatically figure out the metadata version to use::

    CALL example.system.register_table(schema_name => 'testdb', table_name => 'customer_orders', table_location => 'hdfs://hadoop-master:9000/user/hive/warehouse/customer_orders-581fad8517934af6be1857a903559d44', metadata_file_name => '00003-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json')

To prevent unauthorized users from accessing data, this procedure is disabled by default.
The procedure is enabled only when ``iceberg.register-table-procedure.enabled`` is set to ``true``.

.. _iceberg-unregister-table:

Unregister table
----------------
The connector can unregister existing Iceberg tables from the catalog.

The procedure ``system.unregister_table`` allows the caller to unregister an
existing Iceberg table from the metastores without deleting the data::

    CALL example.system.unregister_table(schema_name => 'testdb', table_name => 'customer_orders')

Migrating existing tables
-------------------------

The connector can read from or write to Hive tables that have been migrated to Iceberg.
An SQL procedure ``system.migrate`` allows the caller to replace
a Hive table with an Iceberg table, loaded with the source’s data files.
Table schema, partitioning, properties, and location will be copied from the source table.
Migrate will fail if any table partition uses an unsupported format::

    CALL iceberg.system.migrate(schema_name => 'testdb', table_name => 'customer_orders')

In addition, you can provide a ``recursive_directory`` argument to migrate the table with recursive directories.
The possible values are ``true``, ``false`` and ``fail``. The default value is ``fail`` that throws an exception
if nested directory exists::

    CALL iceberg.system.migrate(schema_name => 'testdb', table_name => 'customer_orders', recursive_directory => 'true')

.. _iceberg-table-properties:

Iceberg table properties
------------------------

================================================== ================================================================
Property name                                      Description
================================================== ================================================================
``format``                                         Optionally specifies the format of table data files;
                                                   either ``PARQUET``, ``ORC`` or ``AVRO``.  Defaults to ``ORC``.

``partitioning``                                   Optionally specifies table partitioning.
                                                   If a table is partitioned by columns ``c1`` and ``c2``, the
                                                   partitioning property would be
                                                   ``partitioning = ARRAY['c1', 'c2']``

``location``                                       Optionally specifies the file system location URI for
                                                   the table.

``format_version``                                 Optionally specifies the format version of the Iceberg
                                                   specification to use for new tables; either ``1`` or ``2``.
                                                   Defaults to ``2``. Version ``2`` is required for row level deletes.

``orc_bloom_filter_columns``                       Comma separated list of columns to use for ORC bloom filter.
                                                   It improves the performance of queries using Equality and IN predicates
                                                   when reading ORC file.
                                                   Requires ORC format.
                                                   Defaults to ``[]``.

``orc_bloom_filter_fpp``                           The ORC bloom filters false positive probability.
                                                   Requires ORC format.
                                                   Defaults to ``0.05``.
================================================== ================================================================

The table definition below specifies format Parquet, partitioning by columns ``c1`` and ``c2``,
and a file system location of ``/var/example_tables/test_table``::

    CREATE TABLE test_table (
        c1 integer,
        c2 date,
        c3 double)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['c1', 'c2'],
        location = '/var/example_tables/test_table')

The table definition below specifies format ORC, bloom filter index by columns ``c1`` and ``c2``,
fpp is 0.05, and a file system location of ``/var/example_tables/test_table``::

    CREATE TABLE test_table (
        c1 integer,
        c2 date,
        c3 double)
    WITH (
        format = 'ORC',
        location = '/var/example_tables/test_table',
        orc_bloom_filter_columns = ARRAY['c1', 'c2'],
        orc_bloom_filter_fpp = 0.05)

.. _iceberg_metadata_columns:

Metadata columns
----------------

In addition to the defined columns, the Iceberg connector automatically exposes
path metadata as a hidden column in each table:

* ``$path``: Full file system path name of the file for this row

* ``$file_modified_time``: Timestamp of the last modification of the file for this row

You can use these columns in your SQL statements like any other column. This
can be selected directly, or used in conditional statements. For example, you
can inspect the file path for each record::

    SELECT *, "$path", "$file_modified_time"
    FROM example.web.page_views;

Retrieve all records that belong to a specific file using ``"$path"`` filter::

    SELECT *
    FROM example.web.page_views
    WHERE "$path" = '/usr/iceberg/table/web.page_views/data/file_01.parquet'

Retrieve all records that belong to a specific file using ``"$file_modified_time"`` filter::

    SELECT *
    FROM example.web.page_views
    WHERE "$file_modified_time" = CAST('2022-07-01 01:02:03.456 UTC' AS timestamp with time zone)

.. _iceberg-metadata-tables:

Metadata tables
---------------

The connector exposes several metadata tables for each Iceberg table.
These metadata tables contain information about the internal structure
of the Iceberg table. You can query each metadata table by appending the
metadata table name to the table name::

   SELECT * FROM "test_table$data"

``$data`` table
^^^^^^^^^^^^^^^

The ``$data`` table is an alias for the Iceberg table itself.

The statement::

    SELECT * FROM "test_table$data"

is equivalent to::

    SELECT * FROM test_table

``$properties`` table
^^^^^^^^^^^^^^^^^^^^^

The ``$properties`` table provides access to general information about Iceberg
table configuration and any additional metadata key/value pairs that the table
is tagged with.

You can retrieve the properties of the current snapshot of the Iceberg
table ``test_table`` by using the following query::

    SELECT * FROM "test_table$properties"

.. code-block:: text

     key                   | value    |
    -----------------------+----------+
    write.format.default   | PARQUET  |

``$history`` table
^^^^^^^^^^^^^^^^^^

The ``$history`` table provides a log of the metadata changes performed on
the Iceberg table.

You can retrieve the changelog of the Iceberg table ``test_table``
by using the following query::

    SELECT * FROM "test_table$history"

.. code-block:: text

     made_current_at                  | snapshot_id          | parent_id            | is_current_ancestor
    ----------------------------------+----------------------+----------------------+--------------------
    2022-01-10 08:11:20 Europe/Vienna | 8667764846443717831  |  <null>              |  true
    2022-01-10 08:11:34 Europe/Vienna | 7860805980949777961  | 8667764846443717831  |  true

The output of the query has the following columns:

.. list-table:: History columns
  :widths: 30, 30, 40
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``made_current_at``
    - ``timestamp(3) with time zone``
    - The time when the snapshot became active
  * - ``snapshot_id``
    - ``bigint``
    - The identifier of the snapshot
  * - ``parent_id``
    - ``bigint``
    - The identifier of the parent snapshot
  * - ``is_current_ancestor``
    - ``boolean``
    - Whether or not this snapshot is an ancestor of the current snapshot


``$snapshots`` table
^^^^^^^^^^^^^^^^^^^^

The ``$snapshots`` table provides a detailed view of snapshots of the
Iceberg table. A snapshot consists of one or more file manifests,
and the complete table contents is represented by the union
of all the data files in those manifests.

You can retrieve the information about the snapshots of the Iceberg table
``test_table`` by using the following query::

    SELECT * FROM "test_table$snapshots"

.. code-block:: text

     committed_at                      | snapshot_id          | parent_id            | operation          |  manifest_list                                                                                                                           |   summary
    ----------------------------------+----------------------+----------------------+--------------------+------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    2022-01-10 08:11:20 Europe/Vienna | 8667764846443717831  |  <null>              |  append            |   hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/snap-8667764846443717831-1-100cf97e-6d56-446e-8961-afdaded63bc4.avro | {changed-partition-count=0, total-equality-deletes=0, total-position-deletes=0, total-delete-files=0, total-files-size=0, total-records=0, total-data-files=0}
    2022-01-10 08:11:34 Europe/Vienna | 7860805980949777961  | 8667764846443717831  |  append            |   hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/snap-7860805980949777961-1-faa19903-1455-4bb8-855a-61a1bbafbaa7.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=1, total-position-deletes=0, added-files-size=442, total-delete-files=0, total-files-size=442, total-records=1, total-data-files=1}


The output of the query has the following columns:

.. list-table:: Snapshots columns
  :widths: 20, 30, 50
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``committed_at``
    - ``timestamp(3) with time zone``
    - The time when the snapshot became active
  * - ``snapshot_id``
    - ``bigint``
    - The identifier for the snapshot
  * - ``parent_id``
    - ``bigint``
    - The identifier for the parent snapshot
  * - ``operation``
    - ``varchar``
    - The type of operation performed on the Iceberg table.
      The supported operation types in Iceberg are:

      * ``append`` when new data is appended
      * ``replace`` when files are removed and replaced without changing the data in the table
      * ``overwrite`` when new data is added to overwrite existing data
      * ``delete`` when data is deleted from the table  and no new data is added
  * - ``manifest_list``
    - ``varchar``
    - The list of avro manifest files containing the detailed information about the snapshot changes.
  * - ``summary``
    - ``map(varchar, varchar)``
    - A summary of the changes made from the previous snapshot to the current snapshot


``$manifests`` table
^^^^^^^^^^^^^^^^^^^^

The ``$manifests`` table provides a detailed overview of the manifests
corresponding to the snapshots performed in the log of the Iceberg table.

You can retrieve the information about the manifests of the Iceberg table
``test_table`` by using the following query::

    SELECT * FROM "test_table$manifests"

.. code-block:: text

     path                                                                                                           | length          | partition_spec_id    | added_snapshot_id     | added_data_files_count  | added_rows_count | existing_data_files_count   | existing_rows_count | deleted_data_files_count    | deleted_rows_count | partitions
    ----------------------------------------------------------------------------------------------------------------+-----------------+----------------------+-----------------------+-------------------------+------------------+-----------------------------+---------------------+-----------------------------+--------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------
     hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/faa19903-1455-4bb8-855a-61a1bbafbaa7-m0.avro |  6277           |   0                  | 7860805980949777961   | 1                       | 100              | 0                           | 0                   | 0                           | 0                  | {{contains_null=false, contains_nan= false, lower_bound=1, upper_bound=1},{contains_null=false, contains_nan= false, lower_bound=2021-01-12, upper_bound=2021-01-12}}


The output of the query has the following columns:

.. list-table:: Manifests columns
  :widths: 30, 30, 40
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``path``
    - ``varchar``
    - The manifest file location
  * - ``length``
    - ``bigint``
    - The manifest file length
  * - ``partition_spec_id``
    - ``integer``
    - The identifier for the partition specification used to write the manifest file
  * - ``added_snapshot_id``
    - ``bigint``
    - The identifier of the snapshot during which this manifest entry has been added
  * - ``added_data_files_count``
    - ``integer``
    - The number of data files with status ``ADDED`` in the manifest file
  * - ``added_rows_count``
    - ``bigint``
    - The total number of rows in all data files with status ``ADDED`` in the manifest file.
  * - ``existing_data_files_count``
    - ``integer``
    - The number of data files with status ``EXISTING`` in the manifest file
  * - ``existing_rows_count``
    - ``bigint``
    - The total number of rows in all data files with status ``EXISTING`` in the manifest file.
  * - ``deleted_data_files_count``
    - ``integer``
    - The number of data files with status ``DELETED`` in the manifest file
  * - ``deleted_rows_count``
    - ``bigint``
    - The total number of rows in all data files with status ``DELETED`` in the manifest file.
  * - ``partitions``
    - ``array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))``
    - Partition range metadata


``$partitions`` table
^^^^^^^^^^^^^^^^^^^^^

The ``$partitions`` table provides a detailed overview of the partitions
of the  Iceberg table.

You can retrieve the information about the partitions of the Iceberg table
``test_table`` by using the following query::

    SELECT * FROM "test_table$partitions"

.. code-block:: text

     partition             | record_count  | file_count    | total_size    |  data
    -----------------------+---------------+---------------+---------------+------------------------------------------------------
    {c1=1, c2=2021-01-12}  |  2            | 2             |  884          | {c3={min=1.0, max=2.0, null_count=0, nan_count=NULL}}
    {c1=1, c2=2021-01-13}  |  1            | 1             |  442          | {c3={min=1.0, max=1.0, null_count=0, nan_count=NULL}}


The output of the query has the following columns:

.. list-table:: Partitions columns
  :widths: 20, 30, 50
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``partition``
    - ``row(...)``
    - A row which contains the mapping of the partition column name(s) to the partition column value(s)
  * - ``record_count``
    - ``bigint``
    - The number of records in the partition
  * - ``file_count``
    - ``bigint``
    - The number of files mapped in the partition
  * - ``total_size``
    - ``bigint``
    - The size of all the files in the partition
  * - ``data``
    - ``row(... row (min ..., max ... , null_count bigint, nan_count bigint))``
    - Partition range metadata

``$files`` table
^^^^^^^^^^^^^^^^

The ``$files`` table provides a detailed overview of the data files in current snapshot of the  Iceberg table.

To retrieve the information about the data files of the Iceberg table ``test_table`` use the following query::

    SELECT * FROM "test_table$files"

.. code-block:: text

     content  | file_path                                                                                                                     | record_count    | file_format   | file_size_in_bytes   |  column_sizes        |  value_counts     |  null_value_counts | nan_value_counts  | lower_bounds                |  upper_bounds               |  key_metadata  | split_offsets  |  equality_ids
    ----------+-------------------------------------------------------------------------------------------------------------------------------+-----------------+---------------+----------------------+----------------------+-------------------+--------------------+-------------------+-----------------------------+-----------------------------+----------------+----------------+---------------
     0        | hdfs://hadoop-master:9000/user/hive/warehouse/test_table/data/c1=3/c2=2021-01-14/af9872b2-40f3-428f-9c87-186d2750d84e.parquet |  1              |  PARQUET      |  442                 | {1=40, 2=40, 3=44}   |  {1=1, 2=1, 3=1}  |  {1=0, 2=0, 3=0}   | <null>            |  {1=3, 2=2021-01-14, 3=1.3} |  {1=3, 2=2021-01-14, 3=1.3} |  <null>        | <null>         |   <null>



The output of the query has the following columns:

.. list-table:: Files columns
  :widths: 25, 30, 45
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``content``
    - ``integer``
    - Type of content stored in the file.
      The supported content types in Iceberg are:

      * ``DATA(0)``
      * ``POSITION_DELETES(1)``
      * ``EQUALITY_DELETES(2)``
  * - ``file_path``
    - ``varchar``
    - The data file location
  * - ``file_format``
    - ``varchar``
    - The format of the data file
  * - ``record_count``
    - ``bigint``
    - The number of entries contained in the data file
  * - ``file_size_in_bytes``
    - ``bigint``
    - The data file size
  * - ``column_sizes``
    - ``map(integer, bigint)``
    - Mapping between the Iceberg column ID and its corresponding size in the file
  * - ``value_counts``
    - ``map(integer, bigint)``
    - Mapping between the Iceberg column ID and its corresponding count of entries in the file
  * - ``null_value_counts``
    - ``map(integer, bigint)``
    - Mapping between the Iceberg column ID and its corresponding count of ``NULL`` values in the file
  * - ``nan_value_counts``
    - ``map(integer, bigint)``
    - Mapping between the Iceberg column ID and its corresponding count of non numerical values in the file
  * - ``lower_bounds``
    - ``map(integer, bigint)``
    - Mapping between the Iceberg column ID and its corresponding lower bound in the file
  * - ``upper_bounds``
    - ``map(integer, bigint)``
    - Mapping between the Iceberg column ID and its corresponding upper bound in the file
  * - ``key_metadata``
    - ``varbinary``
    - Metadata about the encryption key used to encrypt this file, if applicable
  * - ``split_offsets``
    - ``array(bigint)``
    - List of recommended split locations
  * - ``equality_ids``
    - ``array(integer)``
    - The set of field IDs used for equality comparison in equality delete files

``$refs`` table
^^^^^^^^^^^^^^^

The ``$refs`` table provides information about Iceberg references including branches and tags.

You can retrieve the references of the Iceberg table ``test_table`` by using the following query::

    SELECT * FROM "test_table$refs"

.. code-block:: text

    name            | type   | snapshot_id | max_reference_age_in_ms | min_snapshots_to_keep | max_snapshot_age_in_ms |
    ----------------+--------+-------------+-------------------------+-----------------------+------------------------+
    example_tag     | TAG    | 10000000000 | 10000                   | null                  | null                   |
    example_branch  | BRANCH | 20000000000 | 20000                   | 2                     | 30000                  |

The output of the query has the following columns:

.. list-table:: Refs columns
  :widths: 20, 30, 50
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``name``
    - ``varchar``
    - Name of the reference
  * - ``type``
    - ``varchar``
    - Type of the reference, either ``BRANCH`` or ``TAG``
  * - ``snapshot_id``
    - ``bigint``
    - The snapshot ID of the reference
  * - ``max_reference_age_in_ms``
    - ``bigint``
    - The maximum age of the reference before it could be expired.
  * - ``min_snapshots_to_keep``
    - ``integer``
    - For branch only, the minimum number of snapshots to keep in a branch.
  * - ``max_snapshot_age_in_ms``
    - ``bigint``
    - For branch only, the max snapshot age allowed in a branch. Older snapshots in the branch will be expired.

.. _iceberg-materialized-views:

Materialized views
------------------

The Iceberg connector supports :ref:`sql-materialized-view-management`. In the
underlying system each materialized view consists of a view definition and an
Iceberg storage table. The storage table name is stored as a materialized view
property. The data is stored in that storage table.

You can use the :ref:`iceberg-table-properties` to control the created storage
table and therefore the layout and performance. For example, you can use the
following clause with :doc:`/sql/create-materialized-view` to use the ORC format
for the data files and partition the storage per day using the column
``_date``::

    WITH ( format = 'ORC', partitioning = ARRAY['event_date'] )

By default, the storage table is created in the same schema as the materialized
view definition. The ``iceberg.materialized-views.storage-schema`` catalog
configuration property or ``storage_schema`` materialized view property can be
used to specify the schema where the storage table will be created.

Updating the data in the materialized view with
:doc:`/sql/refresh-materialized-view` deletes the data from the storage table,
and inserts the data that is the result of executing the materialized view
query into the existing table. Data is replaced atomically, so users can
continue to query the materialized view while it is being refreshed.
Refreshing a materialized view also stores
the snapshot-ids of all Iceberg tables that are part of the materialized
view's query in the materialized view metadata. When the materialized
view is queried, the snapshot-ids are used to check if the data in the storage
table is up to date. If the data is outdated, the materialized view behaves
like a normal view, and the data is queried directly from the base tables.
Detecting outdated data is possible only when the materialized view uses
Iceberg tables only, or when it uses mix of Iceberg and non-Iceberg tables
but some Iceberg tables are outdated. When the materialized view is based
on non-Iceberg tables, querying it can return outdated data, since the connector
has no information whether the underlying non-Iceberg tables have changed.

Dropping a materialized view with :doc:`/sql/drop-materialized-view` removes
the definition and the storage table.

Table statistics
----------------

The Iceberg connector can collect column statistics using :doc:`/sql/analyze`
statement. This can be disabled using ``iceberg.extended-statistics.enabled``
catalog configuration property, or the corresponding
``extended_statistics_enabled`` session property.

.. _iceberg_analyze:

Updating table statistics
^^^^^^^^^^^^^^^^^^^^^^^^^

If your queries are complex and include joining large data sets,
running :doc:`/sql/analyze` on tables may improve query performance
by collecting statistical information about the data::

    ANALYZE table_name

This query collects statistics for all columns.

On wide tables, collecting statistics for all columns can be expensive.
It is also typically unnecessary - statistics are
only useful on specific columns, like join keys, predicates, or grouping keys. You can
specify a subset of columns to analyzed with the optional ``columns`` property::

    ANALYZE table_name WITH (columns = ARRAY['col_1', 'col_2'])

This query collects statistics for columns ``col_1`` and ``col_2``.

Note that if statistics were previously collected for all columns, they need to be dropped
using :ref:`drop_extended_stats <drop-extended-stats>` command before re-analyzing.
