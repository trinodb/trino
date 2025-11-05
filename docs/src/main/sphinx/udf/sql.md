# SQL user-defined functions

A SQL user-defined function, also known as SQL routine, is a [user-defined
function](/udf) that uses the SQL routine language and statements for the
definition of the function.

## SQL UDF declaration

Declare a SQL UDF using the [](/udf/function) keyword and the following
statements can be used in addition to [built-in functions and
operators](/functions) and other UDFs:

* [](/udf/sql/begin)
* [](/udf/sql/case)
* [](/udf/sql/declare)
* [](/udf/sql/if)
* [](/udf/sql/iterate)
* [](/udf/sql/leave)
* [](/udf/sql/loop)
* [](/udf/sql/repeat)
* [](/udf/sql/return)
* [](/udf/sql/set)
* [](/udf/sql/while)

```{toctree}
:titlesonly: true
:hidden:

sql/examples
sql/begin
sql/case
sql/declare
sql/if
sql/iterate
sql/leave
sql/loop
sql/repeat
sql/return
sql/set
sql/while
```

A minimal example declares the UDF `doubleup` that returns the input integer
value `x` multiplied by two. The example shows declaration as [](udf-inline) and
invocation with the value 21 to yield the result 42:

```sql
WITH
  FUNCTION doubleup(x integer)
    RETURNS integer
    RETURN x * 2
SELECT doubleup(21);
-- 42
```

The same UDF can also be declared as [](udf-catalog).

Find simple examples in each statement documentation, and refer to the
[](/udf/sql/examples) for more complex use cases that combine multiple
statements.

(udf-sql-label)=
## Labels

SQL UDFs can contain labels as markers for a specific block in the declaration
before the following keywords:

* `CASE`
* `IF`
* `LOOP`
* `REPEAT`
* `WHILE`

The label is used to name the block to continue processing with the `ITERATE`
statement or exit the block with the `LEAVE` statement. This flow control is
supported for nested blocks, allowing to continue or exit an outer block, not
just the innermost block. For example, the following snippet uses the label
`top` to name the complete block from `REPEAT` to `END REPEAT`:

```sql
top: REPEAT
  SET a = a + 1;
  IF a <= 3 THEN
    ITERATE top;
  END IF;
  SET b = b + 1;
  UNTIL a >= 10
END REPEAT;
```

Labels can be used with the `ITERATE` and `LEAVE` statements to continue
processing the block or leave the block. This flow control is also supported for
nested blocks and labels.

## Limitations

The following limitations apply to SQL UDFs.

* UDFs must be declared before they are referenced.
* Recursion cannot be declared or processed.
* Mutual recursion can not be declared or processed.
* Queries cannot be processed in a UDF.

Specifically this means that UDFs can not use `SELECT` queries to retrieve
data or any other queries to process data within the UDF. Instead queries can
use UDFs to process data. UDFs only work on data provided as input values and
only provide output data from the `RETURN` statement.