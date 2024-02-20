# ALTER SCHEMA

## Synopsis

```text
ALTER SCHEMA name RENAME TO new_name
ALTER SCHEMA name SET AUTHORIZATION ( user | USER user | ROLE role )
```

## Description

Change the definition of an existing schema.

## Examples

Rename schema `web` to `traffic`:

```
ALTER SCHEMA web RENAME TO traffic
```

Change owner of schema `web` to user `alice`:

```
ALTER SCHEMA web SET AUTHORIZATION alice
```

Allow everyone to drop schema and create tables in schema `web`:

```
ALTER SCHEMA web SET AUTHORIZATION ROLE PUBLIC
```

## See Also

{doc}`create-schema`
