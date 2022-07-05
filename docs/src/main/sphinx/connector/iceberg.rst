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

The connector supports two Iceberg catalog types, you may use either a Hive
metastore service (HMS) or AWS Glue. The catalog type is determined by the
``iceberg.catalog.type`` property, it can be set to either ``HIVE_METASTORE``
or ``GLUE``.

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

Glue catalog
^^^^^^^^^^^^

When using the Glue catalog, the Iceberg connector supports the same
configuration properties as the Hive connector's Glue setup. See
:ref:`AWS Glue metastore configuration<hive-glue-metastore>`.

.. code-block:: text

    connector.name=iceberg
    iceberg.catalog.type=glue


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
    - ``false``
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

.. _iceberg-sql-support:

SQL support
-----------

This connector provides read access and write access to data and metadata in
Iceberg. In addition to the :ref:`globally available <sql-globally-available>`
and :ref:`read operation <sql-read-operations>` statements, the connector
supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/delete`, see also :ref:`iceberg-delete`
* :doc:`/sql/update`
* :ref:`sql-schema-table-management`, see also :ref:`iceberg-tables`
* :ref:`sql-materialized-view-management`, see also
  :ref:`iceberg-materialized-views`
* :ref:`sql-view-management`

.. _iceberg-alter-table-execute:

ALTER TABLE EXECUTE
^^^^^^^^^^^^^^^^^^^

The connector supports the following commands for use with
:ref:`ALTER TABLE EXECUTE <alter-table-execute>`.

optimize
~~~~~~~~

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
~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~

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

.. _iceberg-alter-table-set-properties:

ALTER TABLE SET PROPERTIES
^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector supports modifying the properties on existing tables using
:ref:`ALTER TABLE SET PROPERTIES <alter-table-set-properties>`.

The following table properties can be updated after a table is created:

* ``format``
* ``format_version``
* ``partitioning``

For example, to update a table from v1 of the Iceberg specification to v2:

.. code-block:: sql

    ALTER TABLE table_name SET PROPERTIES format_version = 2;

Or to set the column ``my_new_partition_column`` as a partition column on a table:

.. code-block:: sql

    ALTER TABLE table_name SET PROPERTIES partitioning = ARRAY[<existing partition columns>, 'my_new_partition_column'];

The current values of a table's properties can be shown using :doc:`SHOW CREATE TABLE </sql/show-create-table>`.

.. _iceberg-type-mapping:

Type mapping
------------

Both Iceberg and Trino have types that are not supported by the Iceberg
connector. The following sections explain their type mapping.

Iceberg to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports selecting Iceberg data types. The following table shows the
Iceberg to Trino type mapping:

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
  * - ``STRUCT(...)``
    - ``ROW(...)``
  * - ``LIST(e)``
    - ``ARRAY(e)``
  * - ``MAP(k,v)``
    - ``MAP(k,v)``

Trino to Iceberg type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports creating tables with the following types in Iceberg. The table
shows the mappings from Trino to Iceberg data types:


.. list-table:: Trino to Iceberg type mapping
  :widths: 25, 30, 45
  :header-rows: 1

  * - Trino type
    - Iceberg type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``INTEGER``
    - ``INT``
    -
  * - ``BIGINT``
    - ``LONG``
    -
  * - ``REAL``
    - ``FLOAT``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(6)``
    - ``TIME``
    - Other precisions not supported
  * - ``TIMESTAMP(6)``
    - ``TIMESTAMP``
    - Other precisions not supported
  * - ``TIMESTAMP(6) WITH TIME ZONE``
    - ``TIMESTAMPTZ``
    - Other precisions not supported
  * - ``VARCHAR, VARCHAR(n)``
    - ``STRING``
    -
  * - ``UUID``
    - ``UUID``
    -
  * - ``VARBINARY``
    - ``BINARY``
    -
  * - ``ROW(...)``
    - ``STRUCT(...)``
    - All fields must have a name
  * - ``ARRAY(e)``
    - ``LIST(e)``
    -
  * - ``MAP(k,v)``
    - ``MAP(k,v)``
    -

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

    CREATE TABLE iceberg.testdb.customer_orders (
        order_id BIGINT,
        order_date DATE,
        account_number BIGINT,
        customer VARCHAR,
        country VARCHAR)
    WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country'])

.. _iceberg-delete:

Deletion by partition
^^^^^^^^^^^^^^^^^^^^^

For partitioned tables, the Iceberg connector supports the deletion of entire
partitions if the ``WHERE`` clause specifies filters only on the identity-transformed
partitioning columns, that can match entire partitions. Given the table definition
above, this SQL will delete all partitions for which ``country`` is ``US``::

    DELETE FROM iceberg.testdb.customer_orders
    WHERE country = 'US'

Tables using either v1 or v2 of the Iceberg specification will perform a partition
delete if the ``WHERE`` clause meets these conditions.

Row level deletion
^^^^^^^^^^^^^^^^^^

Tables using v2 of the Iceberg specification support deletion of individual rows
by writing position delete files.

Rolling back to a previous snapshot
-----------------------------------

Iceberg supports a "snapshot" model of data, where table snapshots are
identified by an snapshot IDs.

The connector provides a system snapshots table for each Iceberg table.  Snapshots are
identified by BIGINT snapshot IDs.  You can find the latest snapshot ID for table
``customer_orders`` by running the following command::

    SELECT snapshot_id FROM iceberg.testdb."customer_orders$snapshots" ORDER BY committed_at DESC LIMIT 1

A SQL procedure ``system.rollback_to_snapshot`` allows the caller to roll back
the state of the table to a previous snapshot id::

    CALL iceberg.system.rollback_to_snapshot('testdb', 'customer_orders', 8954597067493422955)

Schema evolution
----------------

Iceberg and the Iceberg connector support schema evolution, with safe
column add, drop, reorder and rename operations, including in nested structures.
Table partitioning can also be changed and the connector can still
query data created before the partitioning change.

Migrating existing tables
-------------------------

The connector can read from or write to Hive tables that have been migrated to Iceberg.
There is no Trino support for migrating Hive tables to Iceberg, so you need to either use
the Iceberg API or Apache Spark.

.. _iceberg-table-properties:

Iceberg table properties
------------------------

================================================== ================================================================
Property name                                      Description
================================================== ================================================================
``format``                                         Optionally specifies the format of table data files;
                                                   either ``PARQUET``, ``ORC`` or ``AVRO```.  Defaults to ``ORC``.

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
and a file system location of ``/var/my_tables/test_table``::

    CREATE TABLE test_table (
        c1 integer,
        c2 date,
        c3 double)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['c1', 'c2'],
        location = '/var/my_tables/test_table')

The table definition below specifies format ORC, bloom filter index by columns ``c1`` and ``c2``,
fpp is 0.05, and a file system location of ``/var/my_tables/test_table``::

    CREATE TABLE test_table (
        c1 integer,
        c2 date,
        c3 double)
    WITH (
        format = 'ORC',
        location = '/var/my_tables/test_table',
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
    FROM iceberg.web.page_views;

Retrieve all records that belong to a specific file using ``"$path"`` filter::

    SELECT *
    FROM iceberg.web.page_views
    WHERE "$path" = '/usr/iceberg/table/web.page_views/data/file_01.parquet'

Retrieve all records that belong to a specific file using ``"$file_modified_time"`` filter::

    SELECT *
    FROM iceberg.web.page_views
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
query into the existing table. Refreshing a materialized view also stores
the snapshot-ids of all tables that are part of the materialized
view's query in the materialized view metadata. When the materialized
view is queried, the snapshot-ids are used to check if the data in the storage
table is up to date. If the data is outdated, the materialized view behaves
like a normal view, and the data is queried directly from the base tables.

.. warning::

    There is a small time window between the commit of the delete and insert,
    when the materialized view is empty. If the commit operation for the insert
    fails, the materialized view remains empty.

Dropping a materialized view with :doc:`/sql/drop-materialized-view` removes
the definition and the storage table.
