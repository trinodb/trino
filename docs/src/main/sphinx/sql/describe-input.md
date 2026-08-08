# DESCRIBE INPUT

## Synopsis

```text
DESCRIBE INPUT statement_name
```

## Description

Lists the input parameters of a prepared statement along with the
position and type of each parameter. Parameter types that cannot be
determined will appear as `unknown`.

## Examples

Prepare and describe a query with three parameters:

```{try-sql}
PREPARE my_select1 FROM
SELECT ? FROM tpch.tiny.nation WHERE regionkey = ? AND name < ?;
---
DESCRIBE INPUT my_select1
```

Prepare and describe a query with no parameters:

```{try-sql}
PREPARE my_select2 FROM
SELECT * FROM tpch.tiny.nation;
---
DESCRIBE INPUT my_select2
```

## See also

{doc}`prepare`
