# Release 380 (6 May 2022)

## General

* Enable automatic [writer scaling](/admin/properties-writer-scaling) by default. ({issue}`10614`)
* Improve performance of joins involving comparisons with the `<`,`<=`, `>`,`>=` operators. ({issue}`12236`)

## Cassandra connector

* Add support for the v5 and v6 protocols. ({issue}`7729`)
* Removes support for v2 protocol. ({issue}`7729`)
* Make the `cassandra.load-policy.use-dc-aware` and `cassandra.load-policy.dc-aware.local-dc`
  catalog configuration properties mandatory. ({issue}`7729`)

## Hive connector

* Support table redirections from Hive to Delta Lake. ({issue}`11550`)
* Allow configuring a default value for the `auto_purge` table property with the
  `hive.auto-purge` catalog property. ({issue}`11749`)
* Allow configuration of the Hive views translation security semantics with the
  `hive.hive-views.run-as-invoker` catalog configuration property. ({issue}`9227`)
* Rename catalog configuration property `hive.translate-hive-views` to
  `hive.hive-views.enabled`. The former name is still accepted. ({issue}`12238`)
* Rename catalog configuration property `hive.legacy-hive-view-translation`
  to `hive.hive-views.legacy-translation`. The former name is still accepted. ({issue}`12238`)
* Rename session property `legacy_hive_view_translation` to
  `hive_views_legacy_translation`. ({issue}`12238`)

## Iceberg connector

* Allow updating tables from the Iceberg v1 table format to v2 with
  `ALTER TABLE ... SET PROPERTIES`. ({issue}`12161`)
* Allow changing the default [file format](iceberg-table-properties) for a table
  with `ALTER TABLE ... SET PROPERTIES`. ({issue}`12161`)
* Prevent potential corruption when a table change is interrupted by networking
  or timeout failures. ({issue}`10462`)

## MongoDB connector

* Add support for [`ALTER TABLE ... RENAME TO ...`](/sql/alter-table). ({issue}`11423`)
* Fix failure when reading decimal values with precision larger than 18. ({issue}`12205`)

## SQL Server connector

* Add support for bulk data insertion. ({issue}`12176`)
