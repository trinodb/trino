# RESET SESSION

## Synopsis

```text
RESET SESSION name
RESET SESSION catalog.name
```

## Description

Reset a {ref}`session property <session-properties-definition>` value to the
default value.

## Examples

```sql
RESET SESSION optimize_hash_generation;
RESET SESSION hive.optimized_reader_enabled;
```

## See also

{doc}`set-session`, {doc}`show-session`
