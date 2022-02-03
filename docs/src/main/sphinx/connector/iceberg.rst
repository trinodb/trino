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
* Access to a Hive metastore service (HMS).
* Network access from the Trino coordinator to the HMS. Hive
  metastore access with the Thrift protocol defaults to using port 9083.

Configuration
-------------

Iceberg supports the same metastore configuration properties as the Hive connector.
At a minimum, ``hive.metastore.uri`` must be configured:

.. code-block:: text

    connector.name=iceberg
    hive.metastore.uri=thrift://localhost:9083

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
    - Authorization checks are enforced using a configuration file whose path
      is specified in the ``security.config-file`` catalog configuration
      property. See :ref:`hive-file-based-authorization` for information on
      the authorzation configuration file.

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
* :ref:`sql-materialized-views-management`, see also
  :ref:`iceberg-materialized-views`
* :ref:`sql-views-management`

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

.. _iceberg-materialized-views:

Materialized views
------------------

The Iceberg connector supports :ref:`sql-materialized-views-management`. In the
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
query into the existing table.

.. warning::

    There is a small time window between the commit of the delete and insert,
    when the materialized view is empty. If the commit operation for the insert
    fails, the materialized view remains empty.

Dropping a materialized view with :doc:`/sql/drop-materialized-view` removes
the definition and the storage table.
