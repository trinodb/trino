===========
Release 306
===========

General Changes
---------------

* Fix planning failure for queries containing a ``LIMIT`` after a global
  aggregation. (:issue:`437`)
* Fix missing column types in ``EXPLAIN`` output. (:issue:`328`)
* Fix accounting of peak revocable memory reservation. (:issue:`413`)
* Fix double memory accounting for aggregations when spilling is active. (:issue:`413`)
* Fix excessive CPU usage that can occur when spilling for window functions. (:issue:`468`)
* Fix incorrect view name displayed by ``SHOW CREATE VIEW``. (:issue:`433`)
* Allow specifying ``NOT NULL`` when creating tables or adding columns. (:issue:`418`)
* Add a config option (``query.stage-count-warning-threshold``) to specify a
  per-query threshold for the number of stages. When this threshold is exceeded,
  a ``TOO_MANY_STAGES`` warning is raised. (:issue:`330`)
* Support session property values with special characters (e.g., comma or equals sign). (:issue:`407`)
* Remove the ``deprecated.legacy-unnest-array-rows`` configuration option.
  The legacy behavior for ``UNNEST`` of arrays containing ``ROW`` values is no
  longer supported. (:issue:`430`)
* Remove the ``deprecated.legacy-row-field-ordinal-access`` configuration option.
  The legacy mechanism for accessing fields of anonymous ``ROW`` types is no longer
  supported. (:issue:`428`)
* Remove the ``deprecated.group-by-uses-equal`` configuration option. The legacy equality
  semantics for ``GROUP BY`` are not longer supported. (:issue:`432`)
* Remove the ``deprecated.legacy-map-subscript``. The legacy behavior for the map subscript
  operator on missing keys is no longer supported. (:issue:`429`)
* Remove the ``deprecated.legacy-char-to-varchar-coercion`` configuration option. The
  legacy coercion rules between ``CHAR`` and ``VARCHAR`` types are no longer
  supported. (:issue:`431`)
* Remove deprecated ``distributed_join`` system property. Use ``join_distribution_type``
  instead. (:issue:`452`)

Hive Connector Changes
----------------------

* Fix calling procedures immediately after startup, before any other queries are run.
  Previously, the procedure call would fail and also cause all subsequent Hive queries
  to fail. (:issue:`414`)
* Improve ORC reader performance for decoding ``REAL`` and ``DOUBLE`` types. (:issue:`465`)

MySQL Connector Changes
-----------------------

* Allow creating or renaming tables, and adding, renaming, or dropping columns. (:issue:`418`)

PostgreSQL Connector Changes
----------------------------

* Fix predicate pushdown for PostgreSQL ``ENUM`` type. (:issue:`408`)
* Allow creating or renaming tables, and adding, renaming, or dropping columns. (:issue:`418`)

Redshift Connector Changes
--------------------------

* Allow creating or renaming tables, and adding, renaming, or dropping columns. (:issue:`418`)

SQL Server Connector Changes
----------------------------

* Allow creating or renaming tables, and adding, renaming, or dropping columns. (:issue:`418`)

Base-JDBC Connector Library Changes
-----------------------------------

* Allow mapping column type to Presto type based on ``Block``. (:issue:`454`)

SPI Changes
-----------

* Deprecate Table Layout APIs. Connectors can opt out of the legacy behavior by implementing
  ``ConnectorMetadata.usesLegacyTableLayouts()``. (:issue:`420`)
* Add support for limit pushdown into connectors via the ``ConnectorMetadata.applyLimit()``
  method. (:issue:`421`)
* Add time spent waiting for resources to ``QueryCompletedEvent``. (:issue:`461`)
