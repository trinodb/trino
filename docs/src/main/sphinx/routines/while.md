# WHILE

## Synopsis

```text
[label :] WHILE condition DO
  statements
END WHILE
```

## Description

The `WHILE` statement is an optional construct in [SQL
routines](/routines/introduction) to allow processing of a block of statements
as long as a condition is met. The condition is validated as a first step of
each iteration.

The expression that defines the `condition` is evaluated at least once. If the
result is `true`, processing moves to `DO`, through following `statements` and
back to `WHILE` and the `condition`. If the result is `false`, processing moves
to `END WHILE`  and continues with the next statement in the function.

The optional `label` before the `WHILE` keyword can be used to [name the
block](routine-label).

Note that a `WHILE` statement is very similar, with the difference that for
`REPEAT` the statements are processed at least once, and for `WHILE` blocks the
statements might not be processed at all.

## Examples

```sql
WHILE p > 1 DO
  SET r = r * n;
  SET p = p - 1;
END WHILE;
```

Further examples of varying complexity that cover usage of the `WHILE` statement
in combination with other statements are available in the [SQL routines examples
documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [](/routines/loop)
* [](/routines/repeat)
