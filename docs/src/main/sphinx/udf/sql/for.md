# FOR

## Synopsis

```text
[label :] FOR identifier IN lower TO upper [BY step] DO
  statements
END FOR
```

## Description

The `FOR` statement is an optional construct in [](/udf/sql) to allow
processing of a block of statements once for each value in a range of
numbers.

`identifier` names a counter variable, implicitly declared by the `FOR`
statement and scoped to the loop body only. `lower`, `upper`, and the
optional `step` are numeric expressions, evaluated once when the loop starts.
The counter starts at the value of `lower`.

If `step` is omitted, it defaults to `1`. When `step` is positive, the loop
processes `statements` once for each value from `lower` to `upper`
inclusive, adding `step` to the counter after every iteration, and stops as
soon as the counter is greater than `upper`. When `step` is negative, the
loop counts down instead, and stops as soon as the counter is less than
`upper`. A `step` of `0`, or one whose sign contradicts the direction implied
by `lower` and `upper`, results in `statements` not being processed at all.

The optional `label` before the `FOR` keyword can be used to [name the
block](udf-sql-label).

## Examples

The following SQL UDF sums the integers from `1` to `n`:

```sql
WITH
  FUNCTION sum_range(n bigint)
  RETURNS bigint
  BEGIN
    DECLARE total bigint DEFAULT 0;
    FOR i IN 1 TO n DO
      SET total = total + i;
    END FOR;
    RETURN total;
  END
SELECT sum_range(10) AS ten,
       sum_range(100) AS one_hundred
```

The following SQL UDF uses a negative `step` to count down, and a label with
`LEAVE` to stop early once the running total exceeds `limit`:

```sql
WITH
  FUNCTION countdown_until(n bigint, limit bigint)
  RETURNS bigint
  BEGIN
    DECLARE total bigint DEFAULT 0;
    outer: FOR i IN n TO 1 BY -1 DO
      IF total > limit THEN
        LEAVE outer;
      END IF;
      SET total = total + i;
    END FOR;
    RETURN total;
  END
SELECT countdown_until(10, 15) AS example
```

Further examples of varying complexity that cover usage of the `FOR`
statement in combination with other statements are available in the
[](/udf/sql/examples).

## See also

* [](/udf/sql)
* [](/udf/sql/loop)
* [](/udf/sql/while)
* [](/udf/sql/repeat)
