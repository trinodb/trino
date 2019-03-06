===========
Release 304
===========

General Changes
---------------

* Fix wrong results for queries involving ``FULL OUTER JOIN`` and ``coalesce`` expressions
  over the join keys. (:issue:`288`)
* Fix failure when a column is referenced using its fully qualified form. (:issue:`250`)
* Correctly report physical and internal network position count for operators. (:issue:`271`)
* Improve plan stability for repeated executions of the same query. (:issue:`226`)
* Remove deprecated ``datasources`` configuration property. (:issue:`306`)
* Improve error message when a query contains zero-length delimited identifiers. (:issue:`249`)
* Avoid opening an unnecessary HTTP listener on an arbitrary port. (:issue:`239`)
* Add experimental support for spilling for queries involving ``ORDER BY`` or window functions. (:issue:`228`)

Server RPM Changes
------------------

* Preserve modified configuration files when the RPM is uninstalled. (:issue:`267`)

Web UI Changes
--------------

* Fix broken timeline view. (:issue:`283`)
* Show data size and position count reported by connectors and by worker-to-worker data transfers
  in detailed query view. (:issue:`271`)

Hive Connector Changes
----------------------

* Fix authorization failure when using SQL Standard Based Authorization mode with user identifiers
  that contain capital letters. (:issue:`289`)
* Fix wrong results when filtering on the hidden ``$bucket`` column for tables containing
  partitions with different bucket counts. Instead, queries will now fail in this case. (:issue:`286`)
* Record the configured Hive time zone when writing ORC files. (:issue:`212`)
* Use the time zone recorded in ORC files when reading timestamps.
  The configured Hive time zone, which was previously always used, is now
  used only as a default when the writer did not record the time zone. (:issue:`212`)
* Support Parquet files written with Parquet 1.9+ that use ``DELTA_BINARY_PACKED``
  encoding with the Parquet ``INT64`` type. (:issue:`334`)
* Allow setting the retry policy for the Thrift metastore client using the
  ``hive.metastore.thrift.client.*`` configuration properties. (:issue:`240`)
* Reduce file system read operations when reading Parquet file footers. (:issue:`296`)
* Allow ignoring Glacier objects in S3 rather than failing the query. This is
  disabled by default, as it may skip data that is expected to exist, but it can
  be enabled using the ``hive.s3.skip-glacier-objects`` configuration property. (:issue:`305`)
* Add procedure ``system.sync_partition_metadata()`` to synchronize the partitions
  in the metastore with the partitions that are physically on the file system. (:issue:`223`)
* Improve performance of ORC reader for columns that only contain nulls. (:issue:`229`)

PostgreSQL Connector Changes
----------------------------

* Map PostgreSQL ``json`` and ``jsonb`` types to Presto ``json`` type. (:issue:`81`)

Cassandra Connector Changes
---------------------------

* Support queries over tables containing partitioning columns of any type. (:issue:`252`)
* Support ``smallint``, ``tinyint`` and  ``date`` Cassandra types. (:issue:`141`)
