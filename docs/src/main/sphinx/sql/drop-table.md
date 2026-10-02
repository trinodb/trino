# DROP TABLE

## Synopsis

```text
DROP TABLE  [ IF EXISTS ] table_name
```

## Description

Drops an existing table.

The optional `IF EXISTS` clause causes the error to be suppressed if the table
does not exist. The error is not suppressed if a Trino view with the same name
exists.

## Examples

Drop the table `orders_by_date`:

```{try-sql}
CREATE TABLE memory.default.orders_by_date (orderdate date, price double);
---
DROP TABLE memory.default.orders_by_date
```

Drop the table `orders_by_date` if it exists:

```{try-sql}
CREATE TABLE memory.default.orders_by_date (orderdate date, price double);
---
DROP TABLE IF EXISTS memory.default.orders_by_date
```

## See also

{doc}`alter-table`, {doc}`create-table`
