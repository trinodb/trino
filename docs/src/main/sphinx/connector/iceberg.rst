=================
Iceberg connector
=================

Overview
--------

Apache Iceberg is an open table format for huge analytic datasets.
The Iceberg connector allows querying data stored in
files written in Iceberg format, as defined in the
`Iceberg Table Spec <https://iceberg.apache.org/spec/>`_. It supports Apache
Iceberg table spec version 1.

The Iceberg table state is maintained in metadata files. All changes to table state
create a new metadata file and replace the old metadata with an atomic swap.
The table metadata file tracks the table schema, partitioning config,
custom properties, and snapshots of the table contents.

Iceberg data files can be stored in either Parquet or ORC format, as
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

.. list-table:: Iceberg configuration properties
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
  * - ``iceberg.max-partitions-per-writer``
    - Maximum number of partitions handled per writer.
    - 100

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

Currently, the Iceberg connector only supports deletion by partition.
This SQL below will fail because the ``WHERE`` clause selects only some of the rows
in the partition::

    DELETE FROM iceberg.testdb.customer_orders
    WHERE country = 'US' AND customer = 'Freds Foods'

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

System tables and columns
-------------------------

The connector supports queries of the table partitions.  Given a table ``customer_orders``,
``SELECT * FROM iceberg.testdb."customer_orders$partitions"`` shows the table partitions, including the minimum
and maximum values for the partition columns.

.. _iceberg-table-properties:

Iceberg table properties
------------------------

================================================== ================================================================
Property Name                                      Description
================================================== ================================================================
``format``                                         Optionally specifies the format of table data files;
                                                   either ``PARQUET`` or ``ORC``.  Defaults to ``ORC``.

``partitioning``                                   Optionally specifies table partitioning.
                                                   If a table is partitioned by columns ``c1`` and ``c2``, the
                                                   partitioning property would be
                                                   ``partitioning = ARRAY['c1', 'c2']``

``location``                                       Optionally specifies the file system location URI for
                                                   the table.
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
    format-version         | 2        |

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

     path                                                                                                           | length          | partition_spec_id    | added_snapshot_id     |  added_data_files_count  | existing_data_files_count   | deleted_data_files_count    | partitions
    ----------------------------------------------------------------------------------------------------------------+-----------------+----------------------+-----------------------+--------------------------+-----------------------------+-----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------
     hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/faa19903-1455-4bb8-855a-61a1bbafbaa7-m0.avro |  6277           |   0                  | 7860805980949777961   |  1                       |   0                         |  0                          |{{contains_null=false, contains_nan= false, lower_bound=1, upper_bound=1},{contains_null=false, contains_nan= false, lower_bound=2021-01-12, upper_bound=2021-01-12}}


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
  * - ``existing_data_files_count``
    - ``integer``
    - The number of data files with status ``EXISTING`` in the manifest file
  * - ``deleted_data_files_count``
    - ``integer``
    - The number of data files with status ``DELETED`` in the manifest file
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
    -----------------------+---------------+---------------+---------------+--------------------------------------
    {c1=1, c2=2021-01-12}  |  2            | 2             |  884          | {c3={min=1.0, max=2.0, null_count=0}}
    {c1=1, c2=2021-01-13}  |  1            | 1             |  442          | {c3={min=1.0, max=1.0, null_count=0}}


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
    - ``row(... row (min ..., max ... , null_count bigint))``
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

Updating the data in the materialized view with
:doc:`/sql/refresh-materialized-view` deletes the data from the storage table,
and inserts the data that is the result of executing the materialized view
query into the existing table. Refreshing the materialized view will also store
the snapshot-ids of all the tables that are part of the materialized
view's query at that point in materialized view metadata. When the materialized 
view is queried, the snapshot-ids are used to check if the data in the storage 
table is up to date. If the data is outdated, the materialized view will behave 
like a normal view and query the data directly from the base tables.

.. warning::

    There is a small time window between the commit of the delete and insert,
    when the materialized view is empty. If the commit operation for the insert
    fails, the materialized view remains empty.

Dropping a materialized view with :doc:`/sql/drop-materialized-view` removes
the definition and the storage table.
