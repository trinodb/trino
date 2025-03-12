# ITERATE

## Synopsis

```text
ITERATE label
```

## Description

The `ITERATE` statement allows processing of blocks in [](/udf/sql) to move
processing back to the start of a context block. Contexts are defined by a
[`label`](udf-sql-label). If no label is found, the functions fails with an
error message.

## Examples

```sql
FUNCTION count()
RETURNS bigint
BEGIN
  DECLARE a int DEFAULT 0;
  DECLARE b int DEFAULT 0;
  top: REPEAT
    SET a = a + 1;
    IF a <= 3 THEN
        ITERATE top;
    END IF;
    SET b = b + 1;
  RETURN b;
END
```

Further examples of varying complexity that cover usage of the `ITERATE`
statement in combination with other statements are available in the
[](/udf/sql/examples).

## See also

* [](/udf/sql)
* [](/udf/sql/leave)
