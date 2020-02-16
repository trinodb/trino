=========================
Release 330 (XX Feb 2020)
=========================

General Changes
---------------

* Fix incorrect behavior of :func:`format` for ``char`` values. Previously, the function
  did not preserve trailing whitespace of the value being formatted. (:issue:`2629`)
* Fix query failure in some cases when aggregation uses inputs from both sides of a join. (:issue:`2560`)
* Fix query failures when dynamic filtering is on for certain multi-level joins. (:issue:`2659`)
* Fix failure of ``SELECT`` queries when accessing ``information_schema`` schema tables with empty
  value used in predicate (like ``SELECT * FROM catalog.information_schema.tables WHERE table_name=''``).
  (:issue:`2575`)
* Fix query failure when :doc:`/sql/execute` is used with an expression containing a function call. (:issue:`2675`)
* Fix query failure for certain co-located joins when dynamic filtering is enabled. (:issue:`2685`)
* Fix failure in ``SHOW CATALOGS`` when the user does not have permissions to see any catalogs. (:issue:`2593`)
* Improve query performance for some join queries when :doc:`/optimizer/cost-based-optimizations`
  are turned on. (:issue:`2722`)
* Prevent uneven distribution of data that could occur when writing data
  if redistribution or writer scaling is enabled. (:issue:`2788`)
* Add support for ``CREATE VIEW`` with comment (:issue:`2557`)
* Add support for all major geometry types to :func:`ST_Points`. (:issue:`2535`)
* Add ``required_workers_count`` and ``required_workers_max_wait_time`` session properties
  to control the number of workers that must be present in the cluster before the query
  starts. (:issue:`2484`)
* Add ``physical_input_bytes`` column to ``system.runtime.tasks`` table. (:issue:`2803`)
* Verify that the target schema exists for the :doc:`/sql/use` statement. (:issue:`2764`)
* Verify that the session catalog exists when executing :doc:`/sql/set-role`. (:issue:`2768`)


Server Changes
--------------

* Require running on Java 11. This requirement may be temporarily relaxed by adding
  ``-Dpresto-temporarily-allow-java8=true`` to the Presto :ref:`presto_jvm_config`.
  This fallback will be removed in future versions of Presto after March 2020. (:issue:`2751`)
* Add experimental support for running on Linux aarch64 (ARM64). (:issue:`2809`)

Security Changes
----------------

* :ref:`principal-rules` are deprecated and will be removed in a future release.
  These rules have been replaced with :doc:`/security/user-mapping`, which
  specifies how a complex authentication user name is mapped to a simple
  user name for Presto, and :ref:`impersonation-rules` which control the ability
  of a user to impersonate another user. (:issue:`2215`)
* A shared secret is now required when using :doc:`security/internal-communication`.  (:issue:`2202`)
* Kerberos for :doc:`security/internal-communication` has been replaced with the new shared secret mechanism.
  The ``internal-communication.kerberos.enabled`` and ``internal-communication.kerberos.use-canonical-hostname``
  configuration properties must be removed. (:issue:`2202`)
* When authentication is disabled, the Presto user may now be set using standard
  HTTP basic authentication with an empty password.  (:issue:`2653`)

Web UI Changes
--------------

* Display physical read time in detailed query view. (:issue:`2805`)

JDBC Driver Changes
-------------------

* Fix a performance issue on JDK 11+ when connecting using HTTP/2. (:issue:`2633`)
* Implement ``PreparedStatement.setTimestamp`` variant that takes a ``Calendar``. (:issue:`2732`)
* Add ``roles`` property that allows to configure authorization roles to be used for catalogs. (:issue:`2780`)
* Add ``sessionProperties`` property that allows to configure system and catalog session properties. (:issue:`2780`)
* Allow passing ``:`` character within a value of extra credential passed with ``extraCredentials``. (:issue:`2780`)

CLI Changes
-----------

* Fix a performance issue on JDK 11+ when connecting using HTTP/2. (:issue:`2633`)

Cassandra Connector Changes
---------------------------

* Fix query failure when identifiers should be quoted. (:issue:`2455`)

Hive Connector Changes
----------------------

* Fix reading symlinks from kerberized HDFS. (:issue:`2720`)
* Allow using writer scaling with all file formats. Previously, it was not supported for
  text-based, SequenceFile, or Avro formats. (:issue:`2657`)
* Add support for symlink-based tables with Avro files. (:issue:`2720`)
* Allow to ignore partitions that do not have matching data directory. This can be controlled with
  ``hive.ignore-absent-partitions=true`` configuration property or ``<catalog>.ignore_absent_partitions``
  session property. (:issue:`2555`)
* Allow creation of external tables with data, via the ``CREATE TABLE ... AS``, when
  ``hive.non-managed-table-creates-enabled`` and ``hive.non-managed-table-writes-enabled``
  are both set to ``true``. Previously this required executing ``CREATE TABLE`` and ``INSERT``
  as separate statements. (:issue:`2669`)
* Add support for Azure WASB, ADLS Gen1 (ADL) and ADLS Gen2 (ABFS) file systems. (:issue:`2494`)
* Add support for executing basic Hive views. (:issue:`2715`)
* Remove extra file status call after writing text-based, SequenceFile, or Avro file types. (:issue:`1748`)
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
* Add support for ``nested`` data type. (:issue:`754`)

PostgreSQL Connector Changes
----------------------------

* Add read support for PostgreSQL ``money`` data type. The type is mapped to ``varchar`` in Presto.
  (:issue:`2601`)

Other Connector Changes
-----------------------

These changes apply to the MySQL, PostgreSQL, Redshift, Phoenix and SQL Server connectors.

* Respect ``DEFAULT`` column clause when writing to a table. (:issue:`1185`)

SPI Changes
-----------

* Allow procedures to have optional arguments with default values.
  This is available by providing relevant information when invoking
  ``io.prestosql.spi.procedure.Procedure.Argument`` constructor.
  (:issue:`2706`)
* ``SystemAccessControl.checkCanSetUser()`` is is deprecated and has been replaced
  with :doc:`/security/user-mapping` and ``SystemAccessControl.checkCanImpersonateUser()``. (:issue:`2215`)
