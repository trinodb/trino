=========================
Release 330 (18 Feb 2020)
=========================

General
-------

* Fix incorrect behavior of :func:`format` for ``char`` values. Previously, the function
  did not preserve trailing whitespace of the value being formatted. (:issue:`2629`)
* Fix query failure in some cases when aggregation uses inputs from both sides of a join. (:issue:`2560`)
* Fix query failure when dynamic filtering is enabled and the query contains complex
  multi-level joins. (:issue:`2659`)
* Fix query failure for certain co-located joins when dynamic filtering is enabled. (:issue:`2685`)
* Fix failure of ``SHOW`` statements or queries that access ``information_schema`` schema tables
  with an empty value used in a predicate. (:issue:`2575`)
* Fix query failure when :doc:`/sql/execute` is used with an expression containing a function call. (:issue:`2675`)
* Fix failure in ``SHOW CATALOGS`` when the user does not have permissions to see any catalogs. (:issue:`2593`)
* Improve query performance for some join queries when :doc:`/optimizer/cost-based-optimizations`
  are enabled. (:issue:`2722`)
* Prevent uneven distribution of data that can occur when writing data with redistribution or writer
  scaling enabled. (:issue:`2788`)
* Add support for ``CREATE VIEW`` with comment (:issue:`2557`)
* Add support for all major geometry types to :func:`ST_Points`. (:issue:`2535`)
* Add ``required_workers_count`` and ``required_workers_max_wait_time`` session properties
  to control the number of workers that must be present in the cluster before query
  processing starts. (:issue:`2484`)
* Add ``physical_input_bytes`` column to ``system.runtime.tasks`` table. (:issue:`2803`)
* Verify that the target schema exists for the :doc:`/sql/use` statement. (:issue:`2764`)
* Verify that the session catalog exists when executing :doc:`/sql/set-role`. (:issue:`2768`)

Server
------

* Require running on :ref:`Java 11 or above <requirements-java>`. This requirement may be temporarily relaxed by adding
  ``-Dpresto-temporarily-allow-java8=true`` to the Presto :ref:`jvm_config`.
  This fallback will be removed in future versions of Presto after March 2020. (:issue:`2751`)
* Add experimental support for running on Linux aarch64 (ARM64). (:issue:`2809`)

Security
--------

* :ref:`principal_rules` are deprecated and will be removed in a future release.
  These rules have been replaced with :doc:`/security/user-mapping`, which
  specifies how a complex authentication user name is mapped to a simple
  user name for Presto, and :ref:`impersonation_rules` which control the ability
  of a user to impersonate another user. (:issue:`2215`)
* A shared secret is now required when using :doc:`/security/internal-communication`. (:issue:`2202`)
* Kerberos for :doc:`/security/internal-communication` has been replaced with the new shared secret mechanism.
  The ``internal-communication.kerberos.enabled`` and ``internal-communication.kerberos.use-canonical-hostname``
  configuration properties must be removed. (:issue:`2202`)
* When authentication is disabled, the Presto user may now be set using standard
  HTTP basic authentication with an empty password. (:issue:`2653`)

Web UI
------

* Display physical read time in detailed query view. (:issue:`2805`)

JDBC driver
-----------

* Fix a performance issue on JDK 11+ when connecting using HTTP/2. (:issue:`2633`)
* Implement ``PreparedStatement.setTimestamp()`` variant that takes a ``Calendar``. (:issue:`2732`)
* Add ``roles`` property for catalog authorization roles. (:issue:`2780`)
* Add ``sessionProperties`` property for setting system and catalog session properties. (:issue:`2780`)
* Add ``clientTags`` property to set client tags for selecting resource groups. (:issue:`2468`)
* Allow using the ``:`` character within an extra credential value specified via the
  ``extraCredentials`` property. (:issue:`2780`)

CLI
---

* Fix a performance issue on JDK 11+ when connecting using HTTP/2. (:issue:`2633`)

Cassandra connector
-------------------

* Fix query failure when identifiers should be quoted. (:issue:`2455`)

Hive connector
--------------

* Fix reading symlinks from HDFS when using Kerberos. (:issue:`2720`)
* Reduce Hive metastore load when updating partition statistics. (:issue:`2734`)
* Allow redistributing writes for un-bucketed partitioned tables on the
  partition keys, which results in a single writer per partition. This reduces
  memory usage, results in a single file per partition, and allows writing a
  large number of partitions (without hitting the open writer limit). However,
  writing large partitions with a single writer can take substantially longer, so
  this feature should only be enabled when required. To enable this feature, set the
  ``use-preferred-write-partitioning`` system configuration property or the
  ``use_preferred_write_partitioning`` system session property to ``true``. (:issue:`2358`)
* Remove extra file status call after writing text-based, SequenceFile, or Avro file types. (:issue:`1748`)
* Allow using writer scaling with all file formats. Previously, it was not supported for
  text-based, SequenceFile, or Avro formats. (:issue:`2657`)
* Add support for symlink-based tables with Avro files. (:issue:`2720`)
* Add support for ignoring partitions with a non-existent data directory. This can be configured
  using the ``hive.ignore-absent-partitions=true`` configuration property or the
  ``ignore_absent_partitions`` session property. (:issue:`2555`)
* Allow creation of external tables with data via ``CREATE TABLE AS`` when
  both ``hive.non-managed-table-creates-enabled`` and ``hive.non-managed-table-writes-enabled``
  are set to ``true``. Previously this required executing ``CREATE TABLE`` and ``INSERT``
  as separate statement (:issue:`2669`)
* Add support for Azure WASB, ADLS Gen1 (ADL) and ADLS Gen2 (ABFS) file systems. (:issue:`2494`)
* Add experimental support for executing basic Hive views. To enable this feature, the
  ``hive.views-execution.enabled`` configuration property must be set to ``true``. (:issue:`2715`)
* Add :ref:`register_partition <register_partition>` and :ref:`unregister_partition <unregister_partition>`
  procedures for adding partitions to and removing partitions from a partitioned table. (:issue:`2692`)
* Allow running :doc:`/sql/analyze` collecting only basic table statistics. (:issue:`2762`)

Elasticsearch connector
-----------------------

* Improve performance of queries containing a ``LIMIT`` clause. (:issue:`2781`)
* Add support for ``nested`` data type. (:issue:`754`)

PostgreSQL connector
--------------------

* Add read support for PostgreSQL ``money`` data type. The type is mapped to ``varchar`` in Presto.
  (:issue:`2601`)

Other connectors
----------------

These changes apply to the MySQL, PostgreSQL, Redshift, Phoenix and SQL Server connectors.

* Respect ``DEFAULT`` column clause when writing to a table. (:issue:`1185`)

SPI
---

* Allow procedures to have optional arguments with default values. (:issue:`2706`)
* ``SystemAccessControl.checkCanSetUser()`` is deprecated and has been replaced
  with :doc:`/security/user-mapping` and ``SystemAccessControl.checkCanImpersonateUser()``. (:issue:`2215`)
