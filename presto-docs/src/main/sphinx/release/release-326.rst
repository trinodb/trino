=========================
Release 326 (27 Nov 2019)
=========================

General Changes
---------------

* Fix incorrect query results when query contains ``LEFT JOIN`` over ``UNNEST``. (:issue:`2097`)
* Fix performance regression in queries involving ``JOIN``. (:issue:`2047`)
* Fix accounting of semantic analysis time when queued queries are cancelled. (:issue:`2055`)
* Add :doc:`/connector/memsql`. (:issue:`1906`)
* Improve performance of ``INSERT`` and ``CREATE TABLE ... AS`` queries containing redundant
  ``ORDER BY`` clauses. (:issue:`2044`)
* Improve performance when processing columns of ``map`` type. (:issue:`2015`)

Server RPM Changes
------------------

* Allow running Presto with :ref:`Java 11 or above <requirements-java>`. (:issue:`2057`)

Security Changes
----------------

* Deprecate Kerberos in favor of JWT for :doc:`/security/internal-communication`. (:issue:`2032`)

Hive Changes
------------

* Fix table creation error for tables with S3 location when using ``file`` metastore. (:issue:`1664`)
* Fix a compatibility issue with the CDH 5.x metastore which results in stats
  not being recorded for :doc:`/sql/analyze`. (:issue:`973`)
* Improve performance for Glue metastore by fetching partitions in parallel. (:issue:`1465`)
* Improve performance of ``sql-standard`` security. (:issue:`1922`, :issue:`1929`)

Phoenix Connector Changes
-------------------------

* Collect statistics on the count and duration of each call to Phoenix. (:issue:`2024`)

Other Connector Changes
-----------------------

These changes apply to the MySQL, PostgreSQL, Redshift, and SQL Server connectors.

* Collect statistics on the count and duration of operations to create
  and destroy ``JDBC`` connections. (:issue:`2024`)
* Add support for showing column comments. (:issue:`1840`)
