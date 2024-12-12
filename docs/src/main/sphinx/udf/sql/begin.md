# BEGIN

## Synopsis

```text
BEGIN
  [ DECLARE ... ]
  statements
END
```

## Description

Marks the start and end of a block in a [](/udf/sql). `BEGIN` can be used
wherever a statement can be used to group multiple statements together and to
declare variables local to the block. A typical use case is as first statement
within a [](/udf/function). Blocks can also be nested.

After the `BEGIN` keyword, you can add variable declarations using
[](/udf/sql/declare) statements, followed by one or more statements that define
the main body of the SQL UDF, separated by `;`. The following statements can be
used:

* [](/udf/sql/case)
* [](/udf/sql/if)
* [](/udf/sql/iterate)
* [](/udf/sql/leave)
* [](/udf/sql/loop)
* [](/udf/sql/repeat)
* [](/udf/sql/return)
* [](/udf/sql/set)
* [](/udf/sql/while)
* Nested [](/udf/sql/begin) blocks

## Examples

The following example computes the value `42`:

```sql
FUNCTION meaning_of_life()
  RETURNS tinyint
  BEGIN
    DECLARE a tinyint DEFAULT 6;
    DECLARE b tinyint DEFAULT 7;
    RETURN a * b;
  END
```

Further examples of varying complexity that cover usage of the `BEGIN` statement
in combination with other statements are available in the [](/udf/sql/examples).

## See also

* [](/udf)
* [](/udf/sql)
* [](/udf/function)
