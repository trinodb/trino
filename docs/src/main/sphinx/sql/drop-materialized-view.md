# DROP MATERIALIZED VIEW

## Synopsis

```text
DROP MATERIALIZED VIEW [ IF EXISTS ] view_name
```

## Description

Drop an existing materialized view `view_name`.

The optional `IF EXISTS` clause causes the error to be suppressed if
the materialized view does not exist.

## Examples

Drop the materialized view `orders_by_date`:

```
DROP MATERIALIZED VIEW orders_by_date;
```

Drop the materialized view `orders_by_date` if it exists:

```
DROP MATERIALIZED VIEW IF EXISTS orders_by_date;
```

## See also

- {doc}`create-materialized-view`
- {doc}`show-create-materialized-view`
- {doc}`refresh-materialized-view`
