# LOOP

## Synopsis

```text
[label :] LOOP
    statements
END LOOP
```

## Description

The `LOOP` statement is an optional construct in [SQL
routines](/routines/introduction) to allow processing of a block of statements
repeatedly.

The block of statements is processed at least once. After the first, and every
subsequent processing the expression `condidtion` is validated. If the result is
`true`, processing moves to END REPEAT and continues with the next statement in
the function. If the result is `false`, the statements are processed again
repeatedly.

The optional `label` before the `REPEAT` keyword can be used to [name the
block](routine-label).

Note that a `WHILE` statement is very similar, with the difference that for
`REPEAT` the statements are processed at least once, and for `WHILE` blocks the
statements might not be processed at all.


## Examples


The following function counts up to `100` in a loop starting from the input
value `i` and returns the number of incremental steps in the loop to get to
`100`.

```sql
FUNCTION to_one_hundred(i int)
  RETURNS int
  BEGIN
    DECLARE count int DEFAULT 0;
    abc: LOOP
      IF i >= 100 THEN
        LEAVE abc;
      END IF
      SET count = count + 1;
      SET i = i + 1;
    END LOOP;
    RETURN;
  END
```

Further examples of varying complexity that cover usage of the `LOOP` statement
in combination with other statements are available in the [SQL routines examples
documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [](/routines/repeat)
* [](/routines/while)

