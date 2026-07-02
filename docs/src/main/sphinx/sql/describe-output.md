# DESCRIBE OUTPUT

## Synopsis

```text
DESCRIBE OUTPUT statement_name
DESCRIBE OUTPUT ( query )
```

## Description

List the output columns of a prepared statement or a query, including the
column name (or alias), catalog, schema, table, type, type size in
bytes, and a boolean indicating if the column is aliased.

## Examples

Prepare and describe a query with four output columns:

```{try-sql}
PREPARE my_select1 FROM
SELECT * FROM tpch.tiny.nation;
---
DESCRIBE OUTPUT my_select1
```

Prepare and describe a query whose output columns are expressions:

```{try-sql}
PREPARE my_select2 FROM
SELECT count(*) as my_count, 1+2 FROM tpch.tiny.nation;
---
DESCRIBE OUTPUT my_select2
```

Prepare and describe a row count query:

```{try-sql}
PREPARE my_create FROM
CREATE TABLE memory.default.foo AS SELECT * FROM tpch.tiny.nation;
---
DESCRIBE OUTPUT my_create
```

You can also describe a query directly:
```sql
DESCRIBE OUTPUT (SELECT *, n_name AS "name" FROM nation);
```
```text
 Column Name | Catalog | Schema | Table  |     Type     | Type Size | Aliased
-------------+---------+--------+--------+--------------+-----------+---------
 n_nationkey | tpch    | sf1    | nation | bigint       |         8 | false
 n_name      | tpch    | sf1    | nation | varchar(25)  |         0 | false
 n_regionkey | tpch    | sf1    | nation | bigint       |         8 | false
 n_comment   | tpch    | sf1    | nation | varchar(152) |         0 | false
 name        | tpch    | sf1    | nation | varchar(25)  |         0 | true
(5 rows)
```
## See also

{doc}`prepare`
