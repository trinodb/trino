# Release 442 (14 Mar 2024)

## Delta Lake connector

* Fix query failure when a partition value contains forward slash characters. ({issue}`21030`)

## Hive connector

* Restore support for `SymlinkTextInputFormat` for text formats. ({issue}`21092`)

## Iceberg connector

* Fix large queries failing with a `NullPointerException`. ({issue}`21074`)

## OpenSearch connector

* Add support for configuring AWS deployment type with the
  `opensearch.aws.deployment-type` configuration property. ({issue}`21059`)
