# REPEAT

## Synopsis

```text
[label :] REPEAT
    statements
UNTIL condition
END REPEAT
```

## Description

The `REPEAT UNTIL` statement is an optional construct in [](/udf/sql) to allow
processing of a block of statements as long as a condition is met. The condition
is validated as a last step of each iteration.

The block of statements is processed at least once. After the first, and every
subsequent processing the expression `condidtion` is validated. If the result is
`true`, processing moves to `END REPEAT` and continues with the next statement in
the function. If the result is `false`, the statements are processed again.

The optional `label` before the `REPEAT` keyword can be used to [name the
block](udf-sql-label).

Note that a `WHILE` statement is very similar, with the difference that for
`REPEAT` the statements are processed at least once, and for `WHILE` blocks the
statements might not be processed at all.

## Examples

The following SQL UDF shows a UDF with a `REPEAT` statement that runs until
the value of `a` is greater or equal to `10`.

```{try-sql}
WITH
  FUNCTION test_repeat(a bigint)
  RETURNS bigint
  BEGIN
    REPEAT
      SET a = a + 1;
    UNTIL a >= 10
    END REPEAT;
    RETURN a;
  END
SELECT test_repeat(5)  AS five,
       test_repeat(9)  AS nine,
       test_repeat(10) AS ten,
       test_repeat(11) AS eleven,
       test_repeat(12) AS twelve
```

Since `a` is also the input value and it is increased before the check the
UDF always returns `10` for input values of `9` or less, and the input value
+ 1 for all higher values.

Further examples of varying complexity that cover usage of the `REPEAT`
statement in combination with other statements are available in the
[](/udf/sql/examples).

## See also

* [](/udf/sql)
* [](/udf/sql/loop)
* [](/udf/sql/while)
