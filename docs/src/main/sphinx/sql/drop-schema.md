# DROP SCHEMA

## Synopsis

```text
DROP SCHEMA [ IF EXISTS ] schema_name [ CASCADE | RESTRICT ]
```

## Description

Drop an existing schema. The schema must be empty.

The optional `IF EXISTS` clause causes the error to be suppressed if
the schema does not exist.

## Examples

Drop the schema `web`:

```
DROP SCHEMA web
```

Drop the schema `sales` if it exists:

```
DROP SCHEMA IF EXISTS sales
```

Drop the schema `archive`, along with everything it contains:

```
DROP SCHEMA archive CASCADE
```

Drop the schema `archive`, only if there are no objects contained in the schema:

```
DROP SCHEMA archive RESTRICT
```

## See also

{doc}`alter-schema`, {doc}`create-schema`
