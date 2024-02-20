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

The block of `statements` is processed until an explicit use of `LEAVE` causes
processing to exit the loop. If processing reaches `END LOOP`, another iteration
of processing from the beginning starts. `LEAVE` statements are typically
wrapped in an `IF` statement that declares a condition to stop the loop.

The optional `label` before the `LOOP` keyword can be used to [name the
block](routine-label).

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
* [](/routines/leave)
