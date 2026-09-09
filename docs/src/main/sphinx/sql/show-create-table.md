# SHOW CREATE TABLE

## Synopsis

```text
SHOW CREATE TABLE table_name
```

## Description

Show the SQL statement that creates the specified table.

## Examples

Show the SQL that can be run to create the `orders` table:

```{try-sql}
SHOW CREATE TABLE tpch.sf1.orders
```

## See also

{doc}`create-table`
