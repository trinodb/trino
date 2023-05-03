====================
Delta Lake connector
====================

.. raw:: html

  <img src="../_static/img/delta-lake.png" class="connector-logo">

The Delta Lake connector allows querying data stored in the `Delta Lake
<https://delta.io>`_ format, including `Databricks Delta Lake
<https://docs.databricks.com/delta/index.html>`_. The connector can natively
read the Delta Lake transaction log and thus detect when external systems change
data.

Requirements
------------

To connect to Databricks Delta Lake, you need:

* Tables written by Databricks Runtime 7.3 LTS, 9.1 LTS, 10.4 LTS, 11.3 LTS, and
  12.2 LTS are supported.
* Deployments using AWS, HDFS, Azure Storage, and Google Cloud Storage (GCS) are
  fully supported.
* Network access from the coordinator and workers to the Delta Lake storage.
* Access to the Hive metastore service (HMS) of Delta Lake or a separate HMS.
* Network access to the HMS from the coordinator and workers. Port 9083 is the
  default port for the Thrift protocol used by the HMS.

General configuration
---------------------

The connector requires a Hive metastore for table metadata and supports the same
metastore configuration properties as the :doc:`Hive connector
</connector/hive>`. At a minimum, ``hive.metastore.uri`` must be configured.

The connector recognizes Delta tables created in the metastore by the Databricks
runtime. If non-Delta tables are present in the metastore as well, they are not
visible to the connector.

To configure the Delta Lake connector, create a catalog properties file
``etc/catalog/example.properties`` that references the ``delta_lake``
connector. Update the ``hive.metastore.uri`` with the URI of your Hive metastore
Thrift service:

.. code-block:: properties

    connector.name=delta_lake
    hive.metastore.uri=thrift://example.net:9083

If you are using AWS Glue as Hive metastore, you can simply set the metastore to
``glue``:

.. code-block:: properties

    connector.name=delta_lake
    hive.metastore=glue

The Delta Lake connector reuses certain functionalities from the Hive connector,
including the metastore :ref:`Thrift <hive-thrift-metastore>` and :ref:`Glue
<hive-glue-metastore>` configuration, detailed in the :doc:`Hive connector
documentation </connector/hive>`.

To configure access to S3 and S3-compatible storage, Azure storage, and others,
consult the appropriate section of the Hive documentation:

* :doc:`Amazon S3 </connector/hive-s3>`
* :doc:`Azure storage documentation </connector/hive-azure>`
* :ref:`GCS <hive-google-cloud-storage-configuration>`

Delta Lake general configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following configuration properties are all using reasonable, tested default
values. Typical usage does not require you to configure them.

.. list-table:: Delta Lake configuration properties
    :widths: 30, 55, 15
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``delta.metadata.cache-ttl``
      - Frequency of checks for metadata updates equivalent to transactions to
        update the metadata cache specified in :ref:`prop-type-duration`.
      - ``5m``
    * - ``delta.metadata.cache-size``
      - The maximum number of Delta table metadata entries to cache.
      - ``1000``
    * - ``delta.metadata.live-files.cache-size``
      - Amount of memory allocated for caching information about files. Must
        be specified in :ref:`prop-type-data-size` values such as ``64MB``.
        Default is calculated to 10% of the maximum memory allocated to the JVM.
      -
    * - ``delta.metadata.live-files.cache-ttl``
      - Caching duration for active files that correspond to the Delta Lake
        tables.
      - ``30m``
    * - ``delta.compression-codec``
      - The compression codec to be used when writing new data files.
        Possible values are:

        * ``NONE``
        * ``SNAPPY``
        * ``ZSTD``
        * ``GZIP``

        The equivalent catalog session property is ``compression_codec``.
      - ``SNAPPY``
    * - ``delta.max-partitions-per-writer``
      - Maximum number of partitions per writer.
      - ``100``
    * - ``delta.hide-non-delta-lake-tables``
      - Hide information about tables that are not managed by Delta Lake. Hiding
        only applies to tables with the metadata managed in a Glue catalog, and
        does not apply to usage with a Hive metastore service.
      - ``false``
    * - ``delta.enable-non-concurrent-writes``
      - Enable :ref:`write support <delta-lake-data-management>` for all
        supported file systems. Specifically, take note of the warning about
        concurrency and checkpoints.
      - ``false``
    * - ``delta.default-checkpoint-writing-interval``
      - Default integer count to write transaction log checkpoint entries. If
        the value is set to N, then checkpoints are written after every Nth
        statement performing table writes. The value can be overridden for a
        specific table with the ``checkpoint_interval`` table property.
      - ``10``
    * - ``delta.hive-catalog-name``
      - Name of the catalog to which ``SELECT`` queries are redirected when a
        Hive table is detected.
      -
    * - ``delta.checkpoint-row-statistics-writing.enabled``
      - Enable writing row statistics to checkpoint files.
      - ``true``
    * - ``delta.dynamic-filtering.wait-timeout``
      - Duration to wait for completion of :doc:`dynamic filtering
        </admin/dynamic-filtering>` during split generation.
        The equivalent catalog session property is
        ``dynamic_filtering_wait_timeout``.
      -
    * - ``delta.table-statistics-enabled``
      - Enables :ref:`Table statistics <delta-lake-table-statistics>` for
        performance improvements. The equivalent catalog session property
        is ``statistics_enabled``.
      - ``true``
    * - ``delta.extended-statistics.enabled``
      - Enable statistics collection with :doc:`/sql/analyze` and
        use of extended statistics. The equivalent catalog session property
        is ``extended_statistics_enabled``.
      - ``true``
    * - ``delta.extended-statistics.collect-on-write``
      - Enable collection of extended statistics for write operations.
        The equivalent catalog session property is
        ``extended_statistics_collect_on_write``.
      - ``true``
    * - ``delta.per-transaction-metastore-cache-maximum-size``
      - Maximum number of metastore data objects per transaction in
        the Hive metastore cache.
      - ``1000``
    * - ``delta.delete-schema-locations-fallback``
      - Whether schema locations are deleted when Trino can't
        determine whether they contain external files.
      - ``false``
    * - ``delta.parquet.time-zone``
      - Time zone for Parquet read and write.
      - JVM default
    * - ``delta.target-max-file-size``
      - Target maximum size of written files; the actual size could be larger.
        The equivalent catalog session property is ``target_max_file_size``.
      - ``1GB``
    * - ``delta.unique-table-location``
      - Use randomized, unique table locations.
      - ``true``
    * - ``delta.register-table-procedure.enabled``
      - Enable to allow users to call the ``register_table`` procedure.
      - ``false``
    * - ``delta.vacuum.min-retention``
      - Minimum retention threshold for the files taken into account
        for removal by the :ref:`VACUUM<delta-lake-vacuum>` procedure.
        The equivalent catalog session property is
        ``vacuum_min_retention``.
      - ``7 DAYS``

Catalog session properties
^^^^^^^^^^^^^^^^^^^^^^^^^^

The following table describes :ref:`catalog session properties
<session-properties-definition>` supported by the Delta Lake connector:

.. list-table:: Catalog session properties
    :widths: 40, 60, 20
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``parquet_optimized_reader_enabled``
      - Specifies whether batched column readers are used when reading Parquet
        files for improved performance.
      - ``true``
    * - ``parquet_max_read_block_size``
      - The maximum block size used when reading Parquet files.
      - ``16MB``
    * - ``parquet_writer_block_size``
      - The maximum block size created by the Parquet writer.
      - ``128MB``
    * - ``parquet_writer_page_size``
      - The maximum page size created by the Parquet writer.
      - ``1MB``
    * - ``parquet_writer_batch_size``
      - Maximum number of rows processed by the Parquet writer in a batch.
      - ``10000``
    * - ``projection_pushdown_enabled``
      - Read only projected fields from row columns while performing ``SELECT`` queries
      - ``true``

.. _delta-lake-type-mapping:

Type mapping
------------

Because Trino and Delta Lake each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading or
writing data. Data types might not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

See the `Delta Transaction Log specification
<https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types>`_
for more information about supported data types in the Delta Lake table format
specification.

Delta Lake to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Delta Lake types to the corresponding Trino types following
this table:

.. list-table:: Delta Lake to Trino type mapping
  :widths: 40, 60
  :header-rows: 1

  * - Delta Lake type
    - Trino type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``BYTE``
    - ``TINYINT``
  * - ``SHORT``
    - ``SMALLINT``
  * - ``LONG``
    - ``BIGINT``
  * - ``FLOAT``
    - ``REAL``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
  * - ``STRING``
    - ``VARCHAR``
  * - ``BINARY``
    - ``VARBINARY``
  * - ``DATE``
    - ``DATE``
  * - ``TIMESTAMP``
    - ``TIMESTAMP(3) WITH TIME ZONE``
  * - ``ARRAY``
    - ``ARRAY``
  * - ``MAP``
    - ``MAP``
  * - ``STRUCT(...)``
    - ``ROW(...)``

No other types are supported.

Trino to Delta Lake type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Trino types to the corresponding Delta Lake types following
this table:

.. list-table:: Trino to Delta Lake type mapping
  :widths: 60, 40
  :header-rows: 1

  * - Trino type
    - Delta Lake type
  * - ``BOOLEAN``
    - ``BOOLEAN``
  * - ``INTEGER``
    - ``INTEGER``
  * - ``TINYINT``
    - ``BYTE``
  * - ``SMALLINT``
    - ``SHORT``
  * - ``BIGINT``
    - ``LONG``
  * - ``REAL``
    - ``FLOAT``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
  * - ``VARCHAR``
    - ``STRING``
  * - ``VARBINARY``
    - ``BINARY``
  * - ``DATE``
    - ``DATE``
  * - ``TIMESTAMP(3) WITH TIME ZONE``
    - ``TIMESTAMP``
  * - ``ARRAY``
    - ``ARRAY``
  * - ``MAP``
    - ``MAP``
  * - ``ROW(...)``
    - ``STRUCT(...)``

No other types are supported.

Security
--------

The Delta Lake connector allows you to choose one of several means of providing
authorization at the catalog level. You can select a different type of
authorization check in different Delta Lake catalog files.

.. _delta-lake-authorization:

Authorization checks
^^^^^^^^^^^^^^^^^^^^

Enable authorization checks for the connector by setting the ``delta.security``
property in the catalog properties file. This property must be one of the
security values in the following table:

.. list-table:: Delta Lake security values
  :widths: 30, 60
  :header-rows: 1

  * - Property value
    - Description
  * - ``ALLOW_ALL`` (default value)
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

.. _delta-lake-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
Delta Lake. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :ref:`sql-data-management`, see also :ref:`delta-lake-data-management`
* :ref:`sql-view-management`
* :doc:`/sql/create-schema`, see also :ref:`delta-lake-sql-basic-usage`
* :doc:`/sql/create-table`, see also :ref:`delta-lake-sql-basic-usage`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/alter-table`
* :doc:`/sql/drop-schema`
* :doc:`/sql/show-create-schema`
* :doc:`/sql/show-create-table`
* :doc:`/sql/comment`

.. _delta-lake-sql-basic-usage:

Basic usage examples
^^^^^^^^^^^^^^^^^^^^

The connector supports creating schemas. You can create a schema with or without
a specified location.

You can create a schema with the :doc:`/sql/create-schema` statement and the
``location`` schema property. Tables in this schema are located in a
subdirectory under the schema location. Data files for tables in this schema
using the default location are cleaned up if the table is dropped::

  CREATE SCHEMA example.example_schema
  WITH (location = 's3://my-bucket/a/path');

Optionally, the location can be omitted. Tables in this schema must have a
location included when you create them. The data files for these tables are not
removed if the table is dropped::

  CREATE SCHEMA example.example_schema;


When Delta Lake tables exist in storage but not in the metastore, Trino can be
used to register the tables::

  CREATE TABLE example.default.example_table (
    dummy bigint
  )
  WITH (
    location = '...'
  )

Columns listed in the DDL, such as ``dummy`` in the preceding example, are
ignored. The table schema is read from the transaction log instead. If the
schema is changed by an external system, Trino automatically uses the new
schema.

.. warning::

   Using ``CREATE TABLE`` with an existing table content is deprecated, instead
   use the ``system.register_table`` procedure. The ``CREATE TABLE ... WITH
   (location=...)`` syntax can be temporarily re-enabled using the
   ``delta.legacy-create-table-with-existing-location.enabled`` catalog
   configuration property or
   ``legacy_create_table_with_existing_location_enabled`` catalog session
   property.

If the specified location does not already contain a Delta table, the connector
automatically writes the initial transaction log entries and registers the table
in the metastore. As a result, any Databricks engine can write to the table::

   CREATE TABLE example.default.new_table (id bigint, address varchar);

The Delta Lake connector also supports creating tables using the :doc:`CREATE
TABLE AS </sql/create-table-as>` syntax.

Procedures
^^^^^^^^^^

Use the :doc:`/sql/call` statement to perform data manipulation or
administrative tasks. Procedures are available in the system schema of each
catalog. The following code snippet displays how to call the
``example_procedure`` in the ``examplecatalog`` catalog::

    CALL examplecatalog.system.example_procedure()

.. _delta-lake-register-table:

Register table
""""""""""""""

The connector can register table into the metastore with existing transaction
logs and data files.

The ``system.register_table`` procedure allows the caller to register an
existing Delta Lake table in the metastore, using its existing transaction logs
and data files::

    CALL example.system.register_table(schema_name => 'testdb', table_name => 'customer_orders', table_location => 's3://my-bucket/a/path')

To prevent unauthorized users from accessing data, this procedure is disabled by
default. The procedure is enabled only when
``delta.register-table-procedure.enabled`` is set to ``true``.

.. _delta-lake-unregister-table:

Unregister table
""""""""""""""""
The connector can unregister existing Delta Lake tables from the metastore.

The procedure ``system.unregister_table`` allows the caller to unregister an
existing Delta Lake table from the metastores without deleting the data::

    CALL example.system.unregister_table(schema_name => 'testdb', table_name => 'customer_orders')

.. _delta-lake-flush-metadata-cache:

Flush metadata cache
""""""""""""""""""""

* ``system.flush_metadata_cache()``

  Flushes all metadata caches.

* ``system.flush_metadata_cache(schema_name => ..., table_name => ...)``

  Flushes metadata cache entries of a specific table.
  Procedure requires passing named parameters.

.. _delta-lake-vacuum:

``VACUUM``
""""""""""

The ``VACUUM`` procedure removes all old files that are not in the transaction
log, as well as files that are not needed to read table snapshots newer than the
current time minus the retention period defined by the ``retention period``
parameter.

Users with ``INSERT`` and ``DELETE`` permissions on a table can run ``VACUUM``
as follows:

.. code-block:: shell

  CALL example.system.vacuum('exampleschemaname', 'exampletablename', '7d');

All parameters are required and must be presented in the following order:

* Schema name
* Table name
* Retention period

The ``delta.vacuum.min-retention`` configuration property provides a safety
measure to ensure that files are retained as expected. The minimum value for
this property is ``0s``. There is a minimum retention session property as well,
``vacuum_min_retention``.

.. _delta-lake-write-support:

Updating data
^^^^^^^^^^^^^

You can use the connector to :doc:`/sql/insert`, :doc:`/sql/delete`,
:doc:`/sql/update`, and :doc:`/sql/merge` data in Delta Lake tables.

Write operations are supported for tables stored on the following systems:

* Azure ADLS Gen2, Google Cloud Storage

  Writes to the Azure ADLS Gen2 and Google Cloud Storage are
  enabled by default. Trino detects write collisions on these storage systems
  when writing from multiple Trino clusters, or from other query engines.

* S3 and S3-compatible storage

  Writes to :doc:`Amazon S3 <hive-s3>` and S3-compatible storage must be enabled
  with the ``delta.enable-non-concurrent-writes`` property. Writes to S3 can
  safely be made from multiple Trino clusters; however, write collisions are not
  detected when writing concurrently from other Delta Lake engines. You need to
  make sure that no concurrent data modifications are run to avoid data
  corruption.

.. _delta-lake-data-management:

Data management
^^^^^^^^^^^^^^^

You can use the connector to :doc:`/sql/insert`, :doc:`/sql/delete`,
:doc:`/sql/update`, and :doc:`/sql/merge` data in Delta Lake tables.

Write operations are supported for tables stored on the following systems:

* Azure ADLS Gen2, Google Cloud Storage

  Writes to the Azure ADLS Gen2 and Google Cloud Storage are
  enabled by default. Trino detects write collisions on these storage systems
  when writing from multiple Trino clusters, or from other query engines.

* S3 and S3-compatible storage

  Writes to :doc:`Amazon S3 <hive-s3>` and S3-compatible storage must be enabled
  with the ``delta.enable-non-concurrent-writes`` property. Writes to S3 can
  safely be made from multiple Trino clusters; however, write collisions are not
  detected when writing concurrently from other Delta Lake engines. You must
  make sure that no concurrent data modifications are run to avoid data
  corruption.

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

.. _delta-lake-alter-table-execute:

ALTER TABLE EXECUTE
"""""""""""""""""""

The connector supports the following commands for use with
:ref:`ALTER TABLE EXECUTE <alter-table-execute>`.

optimize
~~~~~~~~

The ``optimize`` command is used for rewriting the content of the specified
table so that it is merged into fewer but larger files. If the table is
partitioned, the data compaction acts separately on each partition selected for
optimization. This operation improves read performance.

All files with a size below the optional ``file_size_threshold`` parameter
(default value for the threshold is ``100MB``) are merged:

.. code-block:: sql

    ALTER TABLE test_table EXECUTE optimize

The following statement merges files in a table that are
under 10 megabytes in size:

.. code-block:: sql

    ALTER TABLE test_table EXECUTE optimize(file_size_threshold => '10MB')

You can use a ``WHERE`` clause with the columns used to partition the table
to filter which partitions are optimized:

.. code-block:: sql

    ALTER TABLE test_partitioned_table EXECUTE optimize
    WHERE partition_key = 1

Table properties
""""""""""""""""
The following table properties are available for use:

.. list-table:: Delta Lake table properties
  :widths: 40, 60
  :header-rows: 1

  * - Property name
    - Description
  * - ``location``
    - File system location URI for the table.
  * - ``partitioned_by``
    - Set partition columns.
  * - ``checkpoint_interval``
    - Set the checkpoint interval in seconds.
  * - ``change_data_feed_enabled``
    - Enables storing change data feed entries.

The following example uses all available table properties::

  CREATE TABLE example.default.example_partitioned_table
  WITH (
    location = 's3://my-bucket/a/path',
    partitioned_by = ARRAY['regionkey'],
    checkpoint_interval = 5,
    change_data_feed_enabled = true
  )
  AS SELECT name, comment, regionkey FROM tpch.tiny.nation;

Metadata tables
"""""""""""""""

The connector exposes several metadata tables for each Delta Lake table.
These metadata tables contain information about the internal structure
of the Delta Lake table. You can query each metadata table by appending the
metadata table name to the table name::

   SELECT * FROM "test_table$history"

``$history`` table
~~~~~~~~~~~~~~~~~~

The ``$history`` table provides a log of the metadata changes performed on
the Delta Lake table.

You can retrieve the changelog of the Delta Lake table ``test_table``
by using the following query::

    SELECT * FROM "test_table$history"

.. code-block:: text

     version |               timestamp               | user_id | user_name |  operation   |         operation_parameters          |                 cluster_id      | read_version |  isolation_level  | is_blind_append
    ---------+---------------------------------------+---------+-----------+--------------+---------------------------------------+---------------------------------+--------------+-------------------+----------------
           2 | 2023-01-19 07:40:54.684 Europe/Vienna | trino   | trino     | WRITE        | {queryId=20230119_064054_00008_4vq5t} | trino-406-trino-coordinator     |            2 | WriteSerializable | true
           1 | 2023-01-19 07:40:41.373 Europe/Vienna | trino   | trino     | ADD COLUMNS  | {queryId=20230119_064041_00007_4vq5t} | trino-406-trino-coordinator     |            0 | WriteSerializable | true
           0 | 2023-01-19 07:40:10.497 Europe/Vienna | trino   | trino     | CREATE TABLE | {queryId=20230119_064010_00005_4vq5t} | trino-406-trino-coordinator     |            0 | WriteSerializable | true

The output of the query has the following history columns:

.. list-table:: History columns
  :widths: 30, 30, 40
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``version``
    - ``bigint``
    - The version of the table corresponding to the operation
  * - ``timestamp``
    - ``timestamp(3) with time zone``
    - The time when the table version became active
  * - ``user_id``
    - ``varchar``
    - The identifier for the user which performed the operation
  * - ``user_name``
    - ``varchar``
    - The username for the user which performed the operation
  * - ``operation``
    - ``varchar``
    - The name of the operation performed on the table
  * - ``operation_parameters``
    - ``map(varchar, varchar)``
    - Parameters of the operation
  * - ``cluster_id``
    - ``varchar``
    - The ID of the cluster which ran the operation
  * - ``read_version``
    - ``bigint``
    - The version of the table which was read in order to perform the operation
  * - ``isolation_level``
    - ``varchar``
    - The level of isolation used to perform the operation
  * - ``is_blind_append``
    - ``boolean``
    - Whether or not the operation appended data

.. _delta-lake-special-columns:

Metadata columns
""""""""""""""""

In addition to the defined columns, the Delta Lake connector automatically
exposes metadata in a number of hidden columns in each table. You can use these
columns in your SQL statements like any other column, e.g., they can be selected
directly or used in conditional statements.

* ``$path``
    Full file system path name of the file for this row.

* ``$file_modified_time``
    Date and time of the last modification of the file for this row.

* ``$file_size``
    Size of the file for this row.

.. _delta-lake-fte-support:

Fault-tolerant execution support
--------------------------------

The connector supports :doc:`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.


Table functions
---------------

The connector provides the following table functions:

table_changes
^^^^^^^^^^^^^

Allows reading Change Data Feed (CDF) entries to expose row-level changes
between two versions of a Delta Lake table. When the ``change_data_feed_enabled``
table property is set to ``true`` on a specific Delta Lake table,
the connector records change events for all data changes on the table.
This is how these changes can be read:

.. code-block:: sql

    SELECT
      *
    FROM
      TABLE(
        system.table_changes(
          schema_name => 'test_schema',
          table_name => 'tableName',
          since_version => 0
        )
      );

``schema_name`` - type ``VARCHAR``, required, name of the schema for which the function is called

``table_name`` - type ``VARCHAR``, required, name of the table for which the function is called

``since_version`` - type ``BIGINT``, optional, version from which changes are shown, exclusive

In addition to returning the columns present in the table, the function
returns the following values for each change event:

* ``_change_type``
    Gives the type of change that occurred. Possible values are ``insert``,
    ``delete``, ``update_preimage`` and ``update_postimage``.

* ``_commit_version``
    Shows the table version for which the change occurred.

* ``_commit_timestamp``
    Represents the timestamp for the commit in which the specified change happened.

This is how it would be normally used:

Create table:

.. code-block:: sql

    CREATE TABLE test_schema.pages (page_url VARCHAR, domain VARCHAR, views INTEGER)
        WITH (change_data_feed_enabled = true);

Insert data:

.. code-block:: sql

    INSERT INTO test_schema.pages
        VALUES
            ('url1', 'domain1', 1),
            ('url2', 'domain2', 2),
            ('url3', 'domain1', 3);
    INSERT INTO test_schema.pages
        VALUES
            ('url4', 'domain1', 400),
            ('url5', 'domain2', 500),
            ('url6', 'domain3', 2);

Update data:

.. code-block:: sql

    UPDATE test_schema.pages
        SET domain = 'domain4'
        WHERE views = 2;

Select changes:

.. code-block:: sql

    SELECT
      *
    FROM
      TABLE(
        system.table_changes(
          schema_name => 'test_schema',
          table_name => 'pages',
          since_version => 1
        )
      )
    ORDER BY _commit_version ASC;

The preceding sequence of SQL statements returns the following result:

.. code-block:: text

  page_url    |     domain     |    views    |    _change_type     |    _commit_version    |    _commit_timestamp
  url4        |     domain1    |    400      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
  url5        |     domain2    |    500      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
  url6        |     domain3    |    2        |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
  url2        |     domain2    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
  url2        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000
  url6        |     domain3    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
  url6        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000

The output shows what changes happen in which version.
For example in version 3 two rows were modified, first one changed from
``('url2', 'domain2', 2)`` into ``('url2', 'domain4', 2)`` and the second from
``('url6', 'domain2', 2)`` into ``('url6', 'domain4', 2)``.

If ``since_version`` is not provided the function produces change events
starting from when the table was created.

.. code-block:: sql

    SELECT
      *
    FROM
      TABLE(
        system.table_changes(
          schema_name => 'test_schema',
          table_name => 'pages'
        )
      )
    ORDER BY _commit_version ASC;

The preceding SQL statement returns the following result:

.. code-block:: text

  page_url    |     domain     |    views    |    _change_type     |    _commit_version    |    _commit_timestamp
  url1        |     domain1    |    1        |    insert           |     1                 |    2023-03-10T20:21:22.000+0000
  url2        |     domain2    |    2        |    insert           |     1                 |    2023-03-10T20:21:22.000+0000
  url3        |     domain1    |    3        |    insert           |     1                 |    2023-03-10T20:21:22.000+0000
  url4        |     domain1    |    400      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
  url5        |     domain2    |    500      |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
  url6        |     domain3    |    2        |    insert           |     2                 |    2023-03-10T21:22:23.000+0000
  url2        |     domain2    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
  url2        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000
  url6        |     domain3    |    2        |    update_preimage  |     3                 |    2023-03-10T22:23:24.000+0000
  url6        |     domain4    |    2        |    update_postimage |     3                 |    2023-03-10T22:23:24.000+0000

You can see changes that occurred at version 1 as three inserts. They are
not visible in the previous statement when ``since_version`` value was set to 1.

Performance
-----------

The connector includes a number of performance improvements detailed in the
following sections:

* Support for :doc:`write partitioning </admin/properties-write-partitioning>`.

.. _delta-lake-table-statistics:

Table statistics
^^^^^^^^^^^^^^^^

Use :doc:`/sql/analyze` statements in Trino to populate data size and
number of distinct values (NDV) extended table statistics in Delta Lake.
The minimum value, maximum value, value count, and null value count
statistics are computed on the fly out of the transaction log of the
Delta Lake table. The :doc:`cost-based optimizer
</optimizer/cost-based-optimizations>` then uses these statistics to improve
query performance.

Extended statistics enable a broader set of optimizations, including join
reordering. The controlling catalog property ``delta.table-statistics-enabled``
is enabled by default. The equivalent :ref:`catalog session property
<session-properties-definition>` is ``statistics_enabled``.

Each ``ANALYZE`` statement updates the table statistics incrementally, so only
the data changed since the last ``ANALYZE`` is counted. The table statistics are
not automatically updated by write operations such as ``INSERT``, ``UPDATE``,
and ``DELETE``. You must manually run ``ANALYZE`` again to update the table
statistics.

To collect statistics for a table, execute the following statement::

  ANALYZE table_schema.table_name;

To recalculate from scratch the statistics for the table use additional parameter ``mode``:

  ANALYZE table_schema.table_name WITH(mode = 'full_refresh');

There are two modes available ``full_refresh`` and ``incremental``.
The procedure use ``incremental`` by default.

To gain the most benefit from cost-based optimizations, run periodic ``ANALYZE``
statements on every large table that is frequently queried.

Fine-tuning
"""""""""""

The ``files_modified_after`` property is useful if you want to run the
``ANALYZE`` statement on a table that was previously analyzed. You can use it to
limit the amount of data used to generate the table statistics:

.. code-block:: SQL

  ANALYZE example_table WITH(files_modified_after = TIMESTAMP '2021-08-23
  16:43:01.321 Z')

As a result, only files newer than the specified time stamp are used in the
analysis.

You can also specify a set or subset of columns to analyze using the ``columns``
property:

.. code-block:: SQL

  ANALYZE example_table WITH(columns = ARRAY['nationkey', 'regionkey'])

To run ``ANALYZE`` with ``columns`` more than once, the next ``ANALYZE`` must
run on the same set or a subset of the original columns used.

To broaden the set of ``columns``, drop the statistics and reanalyze the table.

Disable and drop extended statistics
""""""""""""""""""""""""""""""""""""

You can disable extended statistics with the catalog configuration property
``delta.extended-statistics.enabled`` set to ``false``. Alternatively, you can
disable it for a session, with the :doc:`catalog session property
</sql/set-session>` ``extended_statistics_enabled`` set to ``false``.

If a table is changed with many delete and update operation, calling ``ANALYZE``
does not result in accurate statistics. To correct the statistics, you have to
drop the extended statistics and analyze the table again.

Use the ``system.drop_extended_stats`` procedure in the catalog to drop the
extended statistics for a specified table in a specified schema:

.. code-block::

  CALL example.system.drop_extended_stats('example_schema', 'example_table')

Memory usage
^^^^^^^^^^^^

The Delta Lake connector is memory intensive and the amount of required memory
grows with the size of Delta Lake transaction logs of any accessed tables. It is
important to take that into account when provisioning the coordinator.

You must decrease memory usage by keeping the number of active data files in
the table low by regularly running ``OPTIMIZE`` and ``VACUUM`` in Delta Lake.

Memory monitoring
"""""""""""""""""

When using the Delta Lake connector, you must monitor memory usage on the
coordinator. Specifically, monitor JVM heap utilization using standard tools as
part of routine operation of the cluster.

A good proxy for memory usage is the cache utilization of Delta Lake caches. It
is exposed by the connector with the
``plugin.deltalake.transactionlog:name=<catalog-name>,type=transactionlogaccess``
JMX bean.

You can access it with any standard monitoring software with JMX support, or use
the :doc:`/connector/jmx` with the following query::

  SELECT * FROM jmx.current."*.plugin.deltalake.transactionlog:name=<catalog-name>,type=transactionlogaccess"

Following is an example result:

.. code-block:: text

  datafilemetadatacachestats.hitrate      | 0.97
  datafilemetadatacachestats.missrate     | 0.03
  datafilemetadatacachestats.requestcount | 3232
  metadatacachestats.hitrate              | 0.98
  metadatacachestats.missrate             | 0.02
  metadatacachestats.requestcount         | 6783
  node                                    | trino-master
  object_name                             | io.trino.plugin.deltalake.transactionlog:type=TransactionLogAccess,name=delta

In a healthy system, both ``datafilemetadatacachestats.hitrate`` and
``metadatacachestats.hitrate`` are close to ``1.0``.

.. _delta-lake-table-redirection:

Table redirection
^^^^^^^^^^^^^^^^^

.. include:: table-redirection.fragment

The connector supports redirection from Delta Lake tables to Hive tables
with the ``delta.hive-catalog-name`` catalog configuration property.

Performance tuning configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following table describes performance tuning catalog properties for the
connector.

.. warning::

   Performance tuning configuration properties are considered expert-level
   features. Altering these properties from their default values is likely to
   cause instability and performance degradation. It is strongly suggested that
   you use them only to address non-trivial performance issues, and that you
   keep a backup of the original values if you change them.

.. list-table:: Delta Lake performance tuning configuration properties
    :widths: 30, 50, 20
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``delta.domain-compaction-threshold``
      - Minimum size of query predicates above which Trino compacts the
        predicates. Pushing a large list of predicates down to the data source
        can compromise performance. For optimization in that situation, Trino
        can compact the large predicates. If necessary, adjust the threshold to
        ensure a balance between performance and predicate pushdown.
      - ``100``
    * - ``delta.max-outstanding-splits``
      - The target number of buffered splits for each table scan in a query,
        before the scheduler tries to pause.
      - ``1000``
    * - ``delta.max-splits-per-second``
      - Sets the maximum number of splits used per second to access underlying
        storage. Reduce this number if your limit is routinely exceeded, based
        on your filesystem limits. This is set to the absolute maximum value,
        which results in Trino maximizing the parallelization of data access
        by default. Attempting to set it higher results in Trino not being
        able to start.
      - ``Integer.MAX_VALUE``
    * - ``delta.max-initial-splits``
      - For each query, the coordinator assigns file sections to read first
        at the ``initial-split-size`` until the ``max-initial-splits`` is
        reached. Then it starts issuing reads of the ``max-split-size`` size.
      - ``200``
    * - ``delta.max-initial-split-size``
      - Sets the initial :ref:`prop-type-data-size` for a single read section
        assigned to a worker until ``max-initial-splits`` have been processed.
        You can also use the corresponding catalog session property
        ``<catalog-name>.max_initial_split_size``.
      - ``32MB``
    * - ``delta.max-split-size``
      - Sets the largest :ref:`prop-type-data-size` for a single read section
        assigned to a worker after ``max-initial-splits`` have been processed.
        You can also use the corresponding catalog session property
        ``<catalog-name>.max_split_size``.
      - ``64MB``
    * - ``delta.minimum-assigned-split-weight``
      - A decimal value in the range (0, 1] used as a minimum for weights
        assigned to each split. A low value might improve performance on tables
        with small files. A higher value might improve performance for queries
        with highly skewed aggregations or joins.
      - ``0.05``
    * - ``parquet.max-read-block-row-count``
      - Sets the maximum number of rows read in a batch. The equivalent catalog
        session property is ``parquet_max_read_block_row_count``.
      - ``8192``
    * - ``parquet.optimized-reader.enabled``
      - Specifies whether batched column readers are used when reading Parquet
        files for improved performance. Set this property to ``false`` to
        disable the optimized parquet reader by default. The equivalent catalog
        session property is ``parquet_optimized_reader_enabled``.
      - ``true``
    * - ``parquet.optimized-nested-reader.enabled``
      - Specifies whether batched column readers are used when reading ARRAY,
        MAP, and ROW types from Parquet files for improved performance. Set this
        property to ``false`` to disable the optimized parquet reader by default
        for structural data types. The equivalent catalog session property is
        ``parquet_optimized_nested_reader_enabled``.
      - ``true``
    * - ``parquet.use-column-index``
      - Skip reading Parquet pages by using Parquet column indices. The equivalent
        catalog session property is ``parquet_use_column_index``.
      - ``true``
    * - ``delta.projection-pushdown-enabled``
      - Read only projected fields from row columns while performing ``SELECT`` queries
      - ``true``
