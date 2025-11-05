# LOOP

## Synopsis

```text
[label :] LOOP
    statements
END LOOP
```

## Description

The `LOOP` statement is an optional construct in [](/udf/sql) to allow processing of a block of statements
repeatedly.

The block of `statements` is processed until an explicit use of `LEAVE` causes
processing to exit the loop. If processing reaches `END LOOP`, another iteration
of processing from the beginning starts. `LEAVE` statements are typically
wrapped in an `IF` statement that declares a condition to stop the loop.

The optional `label` before the `LOOP` keyword can be used to [name the
block](udf-sql-label).

## Examples

The following function counts up to `100` with a step size `step` in a loop
starting from the start value `start_value`, and returns the number of
incremental steps in the loop to get to a value of `100` or higher:

```sql
FUNCTION to_one_hundred(start_value int, step int)
  RETURNS int
  BEGIN
    DECLARE count int DEFAULT 0;
    DECLARE current int DEFAULT 0;
    SET current = start_value;
    abc: LOOP
      IF current >= 100 THEN
        LEAVE abc;
      END IF;
      SET count = count + 1;
      SET current = current + step;
    END LOOP;
    RETURN count;
  END
```

Example invocations:

```sql
SELECT to_one_hundred(90, 1); --10
SELECT to_one_hundred(0, 5); --20
SELECT to_one_hundred(12, 3); -- 30
```

Further examples of varying complexity that cover usage of the `LOOP` statement
in combination with other statements are available in the [SQL UDF examples
documentation](/udf/sql/examples).

## See also

* [](/udf/sql)
* [](/udf/sql/leave)
