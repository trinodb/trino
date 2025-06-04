# Release 474 (21 Mar 2025)

## General

* Add `originalUser` and `authenticatedUser` as resource group selectors. ({issue}`24662`)
* Fix a correctness bug in `GROUP BY` or `DISTINCT` queries with a large number
  of unique groups. ({issue}`25381`)

## Docker image

* Use JDK 24 in the runtime. ({issue}`23501`)

## Delta Lake connector

* Fix failure for `MERGE` queries on
  [cloned](https://delta.io/blog/delta-lake-clone/) tables. ({issue}`24756`)

## Iceberg connector

* Add support for setting session timeout on iceberg REST catalog instances with
  the Iceberg catalog configuration property
  `iceberg.rest-catalog.session-timeout`. Defaults to `1h`. ({issue}`25160`)
* Add support for configuring whether OAuth token refreshes are enabled for
  Iceberg REST catalogs with theIceberg catalog configugration property
  `iceberg.rest-catalog.oauth2.token-refresh-enabled`. Defaults to `true`. ({issue}`25160`)

