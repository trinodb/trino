===========
Release 303
===========

General Changes
---------------

* Fix incorrect padding for ``CHAR`` values containing Unicode supplementary characters.
  Previously, such values would be incorrectly padded with too few spaces. (:issue:`195`)
* Fix an issue where a union of a table with a ``VALUES`` statement would execute on a
  single node,  which could lead to out of memory errors. (:issue:`207`)
* Fix ``/v1/info`` to report started status after all plugins have been registered and initialized. (:issue:`213`)
* Improve performance of window functions by avoiding unnecessary data exchanges over the network. (:issue:`177`)
* Choose the distribution type for semi joins based on cost when the
  ``join_distribution_type`` session property is set to ``AUTOMATIC``. (:issue:`160`)
* Expand grouped execution support to window functions, making it possible
  to execute them with less peak memory usage. (:issue:`169`)

Web UI Changes
--------------

* Add additional details to and improve rendering of live plan. (:issue:`182`)

CLI Changes
-----------

* Add ``--progress`` option to show query progress in batch mode. (:issue:`34`)

Hive Connector Changes
----------------------

* Fix query failure when reading Parquet data with no columns selected.
  This affects queries such as ``SELECT count(*)``. (:issue:`203`)

Mongo Connector Changes
-----------------------

* Fix failure for queries involving joins or aggregations on ``ObjectId`` type. (:issue:`215`)


Base-JDBC Connector Library Changes
-----------------------------------

* Allow customizing how query predicates are pushed down to the underlying database. (:issue:`109`)
* Allow customizing how values are written to the underlying database. (:issue:`109`)

SPI Changes
-----------

* Remove deprecated methods ``getSchemaName`` and ``getTableName`` from the ``SchemaTablePrefix``
  class. These were replaced by the ``getSchema`` and ``getTable`` methods. (:issue:`89`)
* Remove deprecated variants of methods ``listTables`` and ``listViews``
  from the ``ConnectorMetadata`` class. (:issue:`89`)
