===========
Release 305
===========

General Changes
---------------

* Fix failure of :doc:`/functions/regexp` for certain patterns and inputs
  when using the default ``JONI`` library. (:issue:`350`)
* Fix a rare ``ClassLoader`` related problem for plugins providing an ``EventListenerFactory``. (:issue:`299`)
* Expose ``join_max_broadcast_table_size`` session property, which was previously hidden. (:issue:`346`)
* Improve performance of queries when spill is enabled but not triggered. (:issue:`315`)
* Consider estimated query peak memory when making cost based decisions. (:issue:`247`)
* Include revocable memory in total memory stats. (:issue:`273`)
* Add peak revocable memory to operator stats. (:issue:`273`)
* Add :func:`ST_Points` function to access vertices of a linestring. (:issue:`316`)
* Add a system table ``system.metadata.analyze_properties``
  to list all :doc:`/sql/analyze` properties. (:issue:`376`)

Resource Groups Changes
-----------------------

* Fix resource group selection when selector uses regular expression variables. (:issue:`373`)

Web UI Changes
--------------

* Display peak revocable memory, current total memory,
  and peak total memory in detailed query view. (:issue:`273`)

CLI Changes
-----------

* Add option to output CSV without quotes. (:issue:`319`)

Hive Connector Changes
----------------------

* Fix handling of updated credentials for Google Cloud Storage (GCS). (:issue:`398`)
* Fix calculation of bucket number for timestamps that contain a non-zero
  milliseconds value. Previously, data would be written into the wrong bucket,
  or could be incorrectly skipped on read. (:issue:`366`)
* Allow writing ORC files compatible with Hive 2.0.0 to 2.2.0 by identifying
  the writer as an old version of Hive (rather than Presto) in the files.
  This can be enabled using the ``hive.orc.writer.use-legacy-version-number``
  configuration property. (:issue:`353`)
* Support dictionary filtering for Parquet v2 files using ``RLE_DICTIONARY`` encoding. (:issue:`251`)
* Remove legacy writers for ORC and RCFile. (:issue:`353`)
* Remove support for the DWRF file format. (:issue:`353`)

Base-JDBC Connector Library Changes
-----------------------------------

* Allow access to extra credentials when opening a JDBC connection. (:issue:`281`)
