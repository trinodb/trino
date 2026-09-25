# TRUNCATE

## Synopsis

```none
TRUNCATE TABLE table_name
```

## Description

Delete all rows from a table.

## Examples

Truncate the table `orders`:

```{try-sql}
CREATE TABLE memory.default.orders (orderkey bigint, orderstatus varchar);
INSERT INTO memory.default.orders VALUES (1, 'OPEN'), (2, 'PROCESSING');
---
TRUNCATE TABLE memory.default.orders
```
