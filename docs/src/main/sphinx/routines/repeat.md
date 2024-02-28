# REPEAT

## Synopsis

```text
[label :] REPEAT
    statements
UNTIL condition
END REPEAT
```

## Description

The `REPEAT UNTIL` statement is an optional construct in [SQL
routines](/routines/introduction) to allow processing of a block of statements
as long as a condition is met. The condition is validated as a last step of each
iteration.

The block of statements is processed at least once. After the first, and every
subsequent processing the expression `condidtion` is validated. If the result is
`true`, processing moves to `END REPEAT` and continues with the next statement in
the function. If the result is `false`, the statements are processed again.

The optional `label` before the `REPEAT` keyword can be used to [name the
block](routine-label).

Note that a `WHILE` statement is very similar, with the difference that for
`REPEAT` the statements are processed at least once, and for `WHILE` blocks the
statements might not be processed at all.

## Examples

The following routine shows a routine with a `REPEAT` statement that runs until
the value of `a` is greater or equal to `10`.

```sql
FUNCTION test_repeat(a bigint)
  RETURNS bigint
  BEGIN
    REPEAT
      SET a = a + 1;
    UNTIL a >= 10
    END REPEAT;
    RETURN a;
  END
```

Since `a` is also the input value and it is increased before the check the
routine always returns `10` for input values of `9` or less, and the input value
+ 1 for all higher values.

Following are a couple of example invocations with result and explanation:

```sql
SELECT test_repeat(5); -- 10
SELECT test_repeat(9); -- 10
SELECT test_repeat(10); -- 11
SELECT test_repeat(11); -- 12
SELECT test_repeat(12); -- 13
```

Further examples of varying complexity that cover usage of the `REPEAT`
statement in combination with other statements are available in the [SQL
routines examples documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [](/routines/loop)
* [](/routines/while)
