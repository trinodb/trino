=========================
Release 330 (XX Feb 2020)
=========================

General Changes
---------------

* Fix incorrect behavior of :func:`format` for ``char`` values. Previously, the function
  did not preserve trailing whitespace of the value being formatted. (:issue:`2629`)
* Fix query failures when dynamic filtering is on for certain multi-level joins. (:issue:`2659`)
* Fix failure of ``SELECT`` queries when accessing ``information_schema`` schema tables with empty
  value used in predicate (like ``SELECT * FROM catalog.information_schema.tables WHERE table_name=''``).
  (:issue:`2575`)
* Fix query failure when :doc:`/sql/execute` is used with an expression containing a function call. (:issue:`2675`)
* Fix dynamic filtering predicate inference for certain co-located joins. (:issue:`2685`)
* Fix failure in ``SHOW CATALOGS`` when the user does not have permissions to see any catalogs. (:issue:`2593`)
* Improve query performance for some join queries when :doc:`cost based optimizations</optimizer/cost-based-optimizations>`
  are turned on. (:issue:`2722`)
* Add support for ``CREATE VIEW`` with comment (:issue:`2557`)
* Add support for all major geometry types to :func:`ST_Points`. (:issue:`2535`)
* Add ``required_workers_count`` and ``required_workers_max_wait_time`` session properties
  to control the number of workers that must be present in the cluster before the query
  starts. (:issue:`2484`)

Cassandra Connector Changes
---------------------------

* Fix query failure when identifiers should be quoted. (:issue:`2455`)

Hive Connector Changes
----------------------

* Allow to ignore partitions that do not have matching data directory. This can be controlled with
  ``hive.ignore-absent-partitions=true`` configuration property or ``<catalog>.ignore_absent_partitions``
  session property. (:issue:`2555`)
* Allow creation of external tables with data, via the ``CREATE TABLE ... AS``, when
  ``hive.non-managed-table-creates-enabled`` and ``hive.non-managed-table-writes-enabled``
  are both set to ``true``. Previously this required executing ``CREATE TABLE`` and ``INSERT``
  as separate statements. (:issue:`2669`)
* Add support for executing basic Hive views. (:issue:`2715`)
* Reduce memory overhead when inserting into partitioned tables.
  This feature can be enabled via ``use_preferred_write_partitioning``
  system session property of ``use-preferred-write-partitioning``
  feature config. (:issue:`2358`)
* Add ``register_partition``, ``unregister_partition`` procedures for adding partitions to and removing
  partitions from a partitioned table. (:issue:`2692`)
* Allow running :doc:`/sql/analyze` collecting only basic table statistics. (:issue:`2762`)

Elasticsearch Connector Changes
-------------------------------

* Improve performance of queries containing a ``LIMIT`` clause. (:issue:`2781`)

PostgreSQL Connector Changes
----------------------------

* Add read support for PostgreSQL ``money`` data type. The type is mapped to ``varchar`` in Presto.
  (:issue:`2601`)

Other Connector Changes
-----------------------

These changes apply to the MySQL, PostgreSQL, Redshift, Phoenix and SQL Server connectors.

* Respect ``DEFAULT`` column clause when writing to a table. (:issue:`1185`)
* Implement ``PreparedStatement.setTimestamp`` variant that takes a ``Calendar``. (:issue:`2732`)

SPI Changes
-----------

* Allow procedures to have optional arguments with default values.
  This is available by providing relevant information when invoking
  ``io.prestosql.spi.procedure.Procedure.Argument`` constructor.
  (:issue:`2706`)
