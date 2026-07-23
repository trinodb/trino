# ALTER MATERIALIZED VIEW

## Synopsis

```text
ALTER MATERIALIZED VIEW [ IF EXISTS ] name RENAME TO new_name
ALTER MATERIALIZED VIEW name SET PROPERTIES property_name = expression [, ...]
ALTER MATERIALIZED VIEW name SET AUTHORIZATION ( user | USER user | ROLE role )
ALTER MATERIALIZED VIEW name EXECUTE command [ ( parameter => expression [, ... ] ) ]
```

## Description

Change the name of an existing materialized view.

The optional `IF EXISTS` clause causes the error to be suppressed if the
materialized view does not exist. The error is not suppressed if the
materialized view does not exist, but a table or view with the given name
exists.

(alter-materialized-view-set-properties)=
### SET PROPERTIES

The `ALTER MATERIALIZED VIEW SET PROPERTIES`  statement followed by some number
of `property_name` and `expression` pairs applies the specified properties
and values to a materialized view. Omitting an already-set property from this
statement leaves that property unchanged in the materialized view.

A property in a `SET PROPERTIES` statement can be set to `DEFAULT`, which
reverts its value back to the default in that materialized view.

Support for `ALTER MATERIALIZED VIEW SET PROPERTIES` varies between
connectors. Refer to the connector documentation for more details.

(alter-materialized-view-execute)=
### EXECUTE

The `ALTER MATERIALIZED VIEW EXECUTE` statement followed by a `command` and
`parameters` performs maintenance on the materialized view's underlying
storage according to the specified command and parameters, without changing
the view's logical contents. It supports the same commands, on a
per-connector basis, as {ref}`ALTER TABLE EXECUTE <alter-table-execute>`,
for example the `optimize` command provided by the
[Iceberg](iceberg-alter-table-execute) connector:

```
ALTER MATERIALIZED VIEW example.test.test_mv EXECUTE optimize(file_size_threshold => '16MB')
```

Running a command against a materialized view requires the privilege to
execute that command against the view itself, not against its underlying
storage table, and not the privilege to refresh the view.

Support for `ALTER MATERIALIZED VIEW EXECUTE` varies between connectors.
Refer to the connector documentation for more details.

## Examples

Rename materialized view `people` to `users` in the current schema:

```
ALTER MATERIALIZED VIEW people RENAME TO users;
```

Rename materialized view `people` to `users`, if materialized view
`people` exists in the current catalog and schema:

```
ALTER MATERIALIZED VIEW IF EXISTS people RENAME TO users;
```

Set view properties (`x = y`) in materialized view `people`:

```
ALTER MATERIALIZED VIEW people SET PROPERTIES x = 'y';
```

Set multiple view properties (`foo = 123` and `foo bar = 456`) in
materialized view `people`:

```
ALTER MATERIALIZED VIEW people SET PROPERTIES foo = 123, "foo bar" = 456;
```

Set view property `x` to its default value in materialized view `people`:

```
ALTER MATERIALIZED VIEW people SET PROPERTIES x = DEFAULT;
```

Change owner of materialized view `people` to user `alice`:

```
ALTER MATERIALIZED VIEW people SET AUTHORIZATION alice
```

Merge small files in the storage of materialized view `people`:

```
ALTER MATERIALIZED VIEW people EXECUTE optimize(file_size_threshold => '16MB')
```

## See also

- {doc}`create-materialized-view`
- {doc}`refresh-materialized-view`
- {doc}`drop-materialized-view`
