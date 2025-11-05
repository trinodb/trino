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
RESET SESSION query_max_run_time;
RESET SESSION hive.optimized_reader_enabled;
```

## See also

{doc}`set-session`, {doc}`show-session`
