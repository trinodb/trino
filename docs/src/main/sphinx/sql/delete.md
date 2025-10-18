# DELETE

## Synopsis

```text
DELETE FROM table_name [ @ branch_name ] [ WHERE condition ]
```

## Description

Delete rows from a table. If the `WHERE` clause is specified, only the
matching rows are deleted. Otherwise, all rows from the table are deleted.

## Examples

Delete all line items shipped by air:

```
DELETE FROM lineitem WHERE shipmode = 'AIR';
```

Delete all line items for low priority orders:

```
DELETE FROM lineitem
WHERE orderkey IN (SELECT orderkey FROM orders WHERE priority = 'LOW');
```

Delete all orders:

```
DELETE FROM orders;
```

Delete all orders in the `audit` branch:

```sql
DELETE FROM orders @ audit;
```

## Limitations

Some connectors have limited or no support for `DELETE`.
See connector documentation for more details.
