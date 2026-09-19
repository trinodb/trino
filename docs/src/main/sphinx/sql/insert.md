# INSERT

## Synopsis

```text
INSERT INTO table_name [ @ branch_name ] [ ( column [, ... ] ) ] query
```

## Description

Insert new rows into a table.

If the list of column names is specified, they must exactly match the list
of columns produced by the query. Each column in the table not present in the
column list will be filled with a `null` value. Otherwise, if the list of
columns is not specified, the columns produced by the query must exactly match
the columns in the table being inserted into.

## Examples

Load additional rows into the `orders` table from the `new_orders` table:

```{try-sql}
CREATE TABLE memory.default.orders (orderkey bigint, orderstatus varchar);
CREATE TABLE memory.default.new_orders (orderkey bigint, orderstatus varchar);
INSERT INTO memory.default.new_orders VALUES (1, 'OPEN'), (2, 'PROCESSING');
---
INSERT INTO memory.default.orders
SELECT * FROM memory.default.new_orders
```

Insert a single row into the `cities` table:

```{try-sql}
CREATE TABLE memory.default.cities (id integer, name varchar);
---
INSERT INTO memory.default.cities VALUES (1, 'San Francisco')
```

Insert multiple rows into the `cities` table:

```{try-sql}
CREATE TABLE memory.default.cities (id integer, name varchar);
---
INSERT INTO memory.default.cities VALUES (2, 'San Jose'), (3, 'Oakland')
```

Insert a single row into the `nation` table with the specified column list:

```{try-sql}
CREATE TABLE memory.default.nation (nationkey bigint, name varchar, regionkey bigint, comment varchar);
---
INSERT INTO memory.default.nation (nationkey, name, regionkey, comment)
VALUES (26, 'POLAND', 3, 'no comment')
```

Insert a row without specifying the `comment` column.
That column will be `null`:

```{try-sql}
CREATE TABLE memory.default.nation (nationkey bigint, name varchar, regionkey bigint, comment varchar);
---
INSERT INTO memory.default.nation (nationkey, name, regionkey)
VALUES (26, 'POLAND', 3)
```

Insert a single row into `audit` branch of the `cities` table:

```sql
INSERT INTO cities @ audit VALUES (1, 'San Francisco');
```

## See also

{doc}`values`
