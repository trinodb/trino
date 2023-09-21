# USE

## Synopsis

```text
USE catalog.schema
USE schema
```

## Description

Update the session to use the specified catalog and schema. If a
catalog is not specified, the schema is resolved relative to the
current catalog.

## Examples

```sql
USE hive.finance;
USE information_schema;
```
