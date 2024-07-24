# SHOW TABLES

## Synopsis

```text
SHOW TABLES [ FROM schema ] [ LIKE pattern ]
```

## Description

List the tables and views in the current schema, for example set with
[](/sql/use) or by a client connection.

Use a fully qualified path to a schema in the form of `catalog_name.schema_name`
to specify any schema in any catalog in the `FROM` clause.

[Specify a pattern](like-operator) in the optional `LIKE` clause to filter
the results to the desired subset.

## Examples

The following query lists tables and views that begin with `p` in
the `tiny` schema of the `tpch` catalog:

```sql
SHOW TABLES FROM tpch.tiny LIKE 'p%';
```

## See also

* [](sql-schema-table-management)
* [](sql-view-management)
