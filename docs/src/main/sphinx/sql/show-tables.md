# SHOW TABLES

## Synopsis

```text
SHOW TABLES [ FROM schema ] [ LIKE pattern ]
```

## Description

List the tables in `schema` or in the current schema.

{ref}`Specify a pattern <like-operator>` in the optional `LIKE` clause to
filter the results to the desired subset.. For example, the following query
allows you to find tables that begin with `p`:

```
SHOW TABLES FROM tpch.tiny LIKE 'p%';
```
