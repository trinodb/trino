=================
Iceberg Connector
=================

Overview
--------

Apache Iceberg is an open table format for huge analytic datasets.
The Iceberg connector allows querying data stored in
files written in Iceberg format, as defined in the
`Iceberg Table Spec <https://iceberg.apache.org/spec/>`_.

The Iceberg table state is maintained in metadata files. All changes to table state
create a new metadata file and replace the old metadata with an atomic swap.
The table metadata file tracks the table schema, partitioning config,
custom properties, and snapshots of the table contents.

Iceberg data files can be stored in either Parquet or ORC format, as
determined by the ``format`` property in the table definition.  The
table ``format`` defaults to ``ORC``.

Iceberg is designed to improve on the known scalability limitations of Hive, which stores
table metadata in a metastore that is backed by a relational database such as MySQL.  It tracks
partition locations in the metastore, but not individual data files.  Presto queries
using the :doc:`/connector/hive` must first call the metastore to get partition locations,
then call the underlying filesystem to list all data files inside each partition,
and then read metadata from each data file.

Since Iceberg stores the paths to data files in the metadata files, it
only consults the underlying file system for files that must be read.

Configuration
-------------

Iceberg supports the same metastore configuration properties as the Hive connector.
At a minimum, ``hive.metastore.uri`` must be configured:

.. code-block:: none

    connector.name=iceberg
    hive.metastore.uri=thrift://localhost:9083

Partitioned Tables
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

In this example, the table is partitioned by month and further divided into 10 buckets based on a hash of the account number::

    CREATE TABLE iceberg.testdb.sample_partitioned (
        order_date DATE,
        account_number BIGINT,
        customer VARCHAR)
    WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)'])

Deletion by Partition
---------------------

For partitioned tables, the Iceberg connector supports the deletion of entire
partitions if the ``WHERE`` clause specifies an identity transform of a partition
column.  Given the table definition above, this SQL will delete all partitions
for which ``order_date`` is in the month of June, 2018::

    DELETE FROM iceberg.testdb.sample_partitioned
    WHERE date_trunc(month, order_date) = date_trunc(month, DATE '2018-06-01')

Currently, the Iceberg connector only supports deletion by partition.
This SQL below will fail because the ``WHERE`` clause selects only some of the rows
in the partition::

    DELETE FROM iceberg.testdb.sample_partitioned
    WHERE date_trunc(month, order_date) = date_trunc(month, DATE '2018-06-01') AND customer = 'Freds Foods'

Rolling Back to a Previous Snapshot
-----------------------------------

Iceberg supports a "snapshot" model of data, where table snapshots are
identified by an snapshot IDs.

The connector provides a system snapshots table for each Iceberg table.  Snapshots are
identified by BIGINT snapshot IDs.  You can find the latest snapshot ID for table
``customer_accounts`` by running the following command::

    SELECT snapshot_id FROM "customer_accounts$snapshots" ORDER BY committed_at DESC LIMIT 1

A SQL procedure ``system.rollback_to_snapshot`` allows the caller to roll back
the state of the table to a previous snapshot id::

    CALL system.rollback_to_snapshot(schema_name, table_name, snapshot_id)

Schema Evolution
----------------

Iceberg and the Iceberg connector support schema evolution, with safe
column add, drop, reorder and rename operations, including in nested structures.
Table partitioning can also be changed and the connector can still
query data created before the partitioning change.

Migrating Existing Tables
-------------------------

The connector can read from or write to Hive tables that have been migrated to Iceberg.
Currently, there is no Presto support to migrate Hive tables to Presto, so you will
need to use either the Iceberg API or Spark.

System Tables and Columns
-------------------------

The connector supports queries of the table partitions.  Given a table ``customer_accounts``,
``SELECT * customer_acccounts$partitions`` shows the table partitions, including the minimum
and maximum values for the partition columns.

Iceberg Table Properties
------------------------

================================================== ================================================================
Property Name                                      Description
================================================== ================================================================
``format``                                         Optionally specifies the format of table data files;
                                                   either ``PARQUET`` or ``ORC`.  Defaults to ``ORC``.

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
