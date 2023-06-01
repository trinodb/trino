# Release 313 (31 May 2019)

## General

- Fix leak in operator peak memory computations. ({issue}`843`)
- Fix incorrect results for queries involving `GROUPING SETS` and `LIMIT`. ({issue}`864`)
- Add compression and encryption support for {doc}`/admin/spill`. ({issue}`778`)

## CLI

- Fix failure when selecting a value of type {ref}`uuid-type`. ({issue}`854`)

## JDBC driver

- Fix failure when selecting a value of type {ref}`uuid-type`. ({issue}`854`)

## Phoenix connector

- Allow matching schema and table names case insensitively. This can be enabled by setting
  the `case-insensitive-name-matching` configuration property to true. ({issue}`872`)
