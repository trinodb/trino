====================
Delta Lake connector
====================

.. raw:: html

  <img src="../_static/img/delta-lake.png" class="connector-logo">

The Delta Lake connector allows querying data stored in `Delta Lake
<https://delta.io>`_ format, including `Databricks Delta Lake
<https://docs.databricks.com/delta/index.html>`_. It can natively read the Delta
transaction log, and thus detect when external systems change data.

Requirements
------------

To connect to Databricks Delta Lake, you need:

* Tables written by Databricks Runtime 7.3 LTS and 9.1 LTS are supported.
* Deployments using AWS, HDFS, and Azure Storage are fully supported. Using
  Google Cloud Storage is not supported.
* Network access from the coordinator and workers to the Delta Lake storage.
* Access to the Hive metastore service (HMS) of Delta Lake or a separate HMS.
* Network access to the HMS from the coordinator and workers. Port 9083 is the
  default port for the Thrift protocol used by the HMS.

Configuration
-------------

The connector requires a Hive metastore for table metadata and supports the same
metastore configuration properties as the :doc:`Hive connector
</connector/hive>`. At a minimum, ``hive.metastore.uri`` must be configured.

The connector recognizes Delta tables created in the metastore by the Databricks
runtime. If non-Delta tables are present in the metastore, as well, they are not
visible to the connector.

To configure the Delta Lake connector, create a catalog properties file, for
example ``etc/catalog/delta.properties``, that references the ``delta-lake``
connector. Update the ``hive.metastore.uri`` with the URI of your Hive metastore
Thrift service:

.. code-block:: properties

    connector.name=delta-lake
    hive.metastore.uri=thrift://example.net:9083

If you are using AWS Glue as Hive metastore, you can simply set the metastore to
``glue``:

.. code-block:: properties

    connector.name=delta-lake
    hive.metastore=glue

The Delta Lake connector reuses certain functionalities from the Hive connector,
including the metastore :ref:`Thrift <hive-thrift-metastore>` and :ref:`Glue
<hive-glue-metastore>` configuration, detailed in the :doc:`Hive connector
documentation </connector/hive>`.

To configure access to S3 and S3-compatible storage, Azure storage, and others,
consult the :doc:`Amazon S3 </connector/hive-s3>` section of the Hive connector
documentation or the :doc:`Azure storage documentation </connector/hive-azure>`,
respectively.

Configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^

The following configuration properties are all using reasonable, tested default
values. Typical usage does not require you to configure them.

.. list-table:: Delta Lake configuration properties
    :widths: 30, 55, 15
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``delta.metadata.cache-ttl``
      - Frequency of checks for metadata updates, equivalent to transactions, to
        update the metadata cache specified in :ref:`prop-type-duration`.
      - ``5m``
    * - ``delta.metadata.live-files.cache-size``
      - Amount of memory allocated for caching information about files. Needs
        to be specified in :ref:`prop-type-data-size` values such as ``64MB``.
        Default is calculated to 10% of the maximum memory allocated to the JVM.
      -
    * - ``delta.max-partitions-per-writer``
      - Maximum number of partitions per writer.
      - 100
    * - ``delta.hide-non-delta-lake-tables``
      - Hide information about tables that are not managed by Delta Lake. Hiding
        only applies to tables with the metadata managed in a Glue catalog, does
        not apply to usage with a Hive metastore service.
      - ``false``
    * - ``delta.enable-non-concurrent-writes``
      - Enable :ref:`write support <delta-lake-write-support>` for all
        supported file systems, specifically take note of the warning about
        concurrency and checkpoints.
      - ``false``
    * - ``delta.default-checkpoint-writing-interval``
      - Default integer count to write transaction log checkpoint entries. If
        the value is set to N, then checkpoints are written after every Nth
        statement performing table writes. The value can be overridden for a
        specific table with the ``checkpoint_interval`` table property.
      - 10
    * - ``delta.hive-catalog-name``
      - Name of the catalog to which ``SELECT`` queries are redirected when a
        Hive table is detected.
      -
    * - ``delta.dynamic-filtering.wait-timeout``
      - Duration to wait for completion of dynamic filters during split generation
      -
    * - ``delta.table-statistics-enabled``
      - Enables :ref:`Table statistics <delta-lake-table-statistics>` for
        performance improvements.
      - ``true``

The following table describes performance tuning catalog properties for the
connector.

.. warning::

   Performance tuning configuration properties are considered expert-level
   features. Altering these properties from their default values is likely to
   cause instability and performance degradation. We strongly suggest that
   you use them only to address non-trivial performance issues, and that you
   keep a backup of the original values if you change them.

.. list-table:: Delta Lake performance tuning configuration properties
    :widths: 30, 50, 20
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``delta.domain-compaction-threshold``
      - Sets the number of transactions to act as threshold. Once reached the
        connector initiates compaction of the underlying files and the delta
        files. A higher compaction threshold means reading less data from the
        underlying data source, but a higher memory and network consumption.
      - 100
    * - ``delta.max-outstanding-splits``
      - The target number of buffered splits for each table scan in a query,
        before the scheduler tries to pause.
      - 1000
    * - ``delta.max-splits-per-second``
      - Sets the maximum number of splits used per second to access underlying
        storage. Reduce this number if your limit is routinely exceeded, based
        on your filesystem limits. This is set to the absolute maximum value,
        which results in Trino maximizing the parallelization of data access
        by default. Attempting to set it higher results in Trino not being
        able to start.
      - Integer.MAX_VALUE
    * - ``delta.max-initial-splits``
      - For each query, the coordinator assigns file sections to read first
        at the ``initial-split-size`` until the ``max-initial-splits`` is
        reached. Then, it starts issuing reads of the ``max-split-size`` size.
      - 200
    * - ``delta.max-initial-split-size``
      - Sets the initial :ref:`prop-type-data-size` for a single read section
        assigned to a worker until ``max-initial-splits`` have been processed.
        You can also use the corresponding catalog session property
        ``<catalog-name>.max_initial_split_size``.
      - ``32MB``
    * - ``delta.max-split-size``
      - Sets the largest :ref:`prop-type-data-size` for a single read section
        assigned to a worker after max-initial-splits have been processed. You
        can also use the corresponding catalog session property
        ``<catalog-name>.max_split_size``.
      - ``64MB``

The following table describes :ref:`catalog session properties
<session-properties-definition>` supported by the Delta Lake connector to
configure processing of Parquet files.

.. list-table:: Parquet catalog session properties
    :widths: 40, 60
    :header-rows: 1

    * - Property name
      - Description
    * - ``parquet_max_read_block_size``
      - The maximum block size used when reading Parquet files.
    * - ``parquet_writer_block_size``
      - The maximum block size created by the Parquet writer.
    * - ``parquet_writer_page_size``
      - The maximum page size created by the Parquet writer.

.. _delta-lake-type-mapping:

Type mapping
------------

Because Trino and Delta Lake each support types that the other does not, this
connector modifies some types when reading or writing data.

Delta Lake to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports selecting Delta Lake data types. The following table shows the
Delta Lake to Trino type mapping:

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

Trino to Delta Lake type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports creating tables with the following types in Delta Lake. The table
shows the mappings from Trino to Delta Lake data types:


.. list-table:: Trino to Delta Lake type mapping
  :widths: 25, 30
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

.. _delta-lake-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
Delta Lake. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :ref:`sql-data-management`, see also :ref:`delta-lake-write-support`
* :doc:`/sql/create-schema`, see also :ref:`delta-lake-create-schema`
* :doc:`/sql/create-table`, see also :ref:`delta-lake-create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/alter-table`
* :doc:`/sql/drop-schema`
* :doc:`/sql/show-create-schema`
* :doc:`/sql/show-create-table`

.. _delta-lake-alter-table-execute:

ALTER TABLE EXECUTE
^^^^^^^^^^^^^^^^^^^

The connector supports the following commands for use with
:ref:`ALTER TABLE EXECUTE <alter-table-execute>`.

optimize
""""""""

The ``optimize`` command is used for rewriting the content
of the specified table so that it is merged into fewer but larger files.
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

.. _delta-lake-special-columns:

Special columns
^^^^^^^^^^^^^^^

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

.. _delta-lake-create-schema:

Creating schemas
^^^^^^^^^^^^^^^^

The connector supports creating schemas. You can create a schema with or without
a specified location.

You can create a schema with the :doc:`/sql/create-schema` statement and the
``location`` schema property. Tables in this schema are located in a
subdirectory under the schema location. Data files for tables in this schema
using the default location are cleaned up if the table is dropped::

  CREATE SCHEMA delta.my_schema
  WITH (location = 's3://my-bucket/a/path');

Optionally, the location can be omitted. Tables in this schema must have a
location included when you create them. The data files for these tables are not
removed if the table is dropped::

  CREATE SCHEMA delta.my_schema;

.. _delta-lake-create-table:

Creating tables
^^^^^^^^^^^^^^^

When Delta tables exist in storage, but not in the metastore, Trino can be used
to register them::

  CREATE TABLE delta.default.my_table (
    dummy bigint
  )
  WITH (
    location = '...'
  )

Columns listed in the DDL, such as ``dummy`` in the preceeding example, are
ignored. The table schema is read from the transaction log, instead. If the
schema is changed by an external system, Trino automatically uses the new
schema.

If the specified location does not already contain a Delta table, the connector
automatically writes the initial transaction log entries and registers the table
in the metastore. As a result, any Databricks engine can write to the table::

   CREATE TABLE delta.default.new_table (id bigint, address varchar);

The Delta Lake connector also supports creating tables using the :doc:`CREATE
TABLE AS </sql/create-table-as>` syntax.

There are three table properties available for use in table creation.

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

The following example uses all three table properties::

  CREATE TABLE delta.default.my_partitioned_table
  WITH (
    location = 's3://my-bucket/a/path',
    partitioned_by = ARRAY['regionkey'],
    checkpoint_interval = 5
  )
  AS SELECT name, comment, regionkey FROM tpch.tiny.nation;

.. _delta-lake-write-support:

Updating data
^^^^^^^^^^^^^

You can use the connector to :doc:`/sql/insert`, :doc:`/sql/delete` and
:doc:`/sql/update` data in Delta Lake tables.

Write operations are supported for tables stored on the following systems:

* Azure ADLS Gen2

  Writes to the Azure ADLS Gen2 file system are enabled by default. Trino
  detects write collisions on ADLS Gen2 when writing from multiple Trino
  clusters, or from as any number of open source Delta Lake clusters.

* S3 and S3-compatible storage

  Writes to :doc:`Amazon S3 <hive-s3>` and S3-compatible storage must be enabled
  with the ``delta.enable-non-concurrent-writes`` property. Writes to S3 can
  safely be made from multiple Trino clusters, however write collisions are not
  detected when writing concurrently from other Delta Lake engines. You need to
  make sure that no concurrent data modifications are run to avoid data
  corruption.

Performance
-----------

The connector includes a number of performance improvements, detailed in the
following sections:

* Support for :doc:`write partitioning </admin/properties-write-partitioning>`.

.. _delta-lake-table-statistics:

Table statistics
^^^^^^^^^^^^^^^^

You can use :doc:`/sql/analyze` statements in Trino to populate the table
statistics in Delta Lake. Number of distinct values (NDV)
statistics are supported, while Minimum value, maximum value, and null value
count statistics are not supported. The :doc:`cost-based optimizer
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

To gain the most benefit from cost-based optimizations, run periodic ``ANALYZE``
statements on every large table that is frequently queried.

Fine tuning
"""""""""""

The ``files_modified_after`` property is useful if you want to run the
``ANALYZE`` statement on a table that was previously analyzed. You can use it to
limit the amount of data used to generate the table statistics:

.. code-block:: SQL

  ANALYZE my_table WITH(files_modified_after = TIMESTAMP '2021-08-23
  16:43:01.321 Z')

As a result, only files newer than the specified time stamp are used in the
analysis.

You can also specify a set or subset of columns to analyze using the ``columns``
property:

.. code-block:: SQL

  ANALYZE my_table WITH(columns = ARRAY['nationkey', 'regionkey'])

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
does not result in accurate statistics. To correct the statistics you have to
drop the extended stats and analyze table again.

Use the ``system.drop_extended_stats`` procedure in the catalog to drop the
extended statistics for a specified table in a specified schema:

.. code-block::

  CALL delta_catalog.system.drop_extended_stats('my_schema', 'my_table')


Memory usage
^^^^^^^^^^^^

The Delta Lake connector is memory intensive and the amount of required memory
grows with the size of Delta Lake transaction logs of any accessed tables. It is
important to take that into account when provisioning the coordinator.

You need to decrease memory usage by keeping the number of active data files in
table low by running ``OPTIMIZE`` and ``VACUUM`` in Delta Lake regularly.

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

  CALL mydeltacatalog.system.vacuum('myschemaname', 'mytablename', '7d');

All parameters are required, and must be presented in the following order:

* Schema name
* Table name
* Retention period

The ``delta.vacuum.min_retention`` config property provides a safety
measure to ensure that files are retained as expected.  The minimum value for
this property is ``0s``. There is a minimum retention session property as well,
``vacuum_min_retention``.

Memory monitoring
"""""""""""""""""

When using the Delta Lake connector you need to monitor memory usage on the
coordinator. Specifically monitor JVM heap utilization using standard tools as
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

In a healthy system both ``datafilemetadatacachestats.hitrate`` and
``metadatacachestats.hitrate`` are close to ``1.0``.
