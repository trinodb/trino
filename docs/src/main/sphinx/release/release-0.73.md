# Release 0.73

## Cassandra plugin

The Cassandra connector now supports CREATE TABLE and DROP TABLE. Additionally,
the connector now takes into account Cassandra indexes when generating CQL.
This release also includes several bug fixes and performance improvements.

## General

- New window functions: {func}`lead`, and {func}`lag`
- New scalar function: {func}`json_size`
