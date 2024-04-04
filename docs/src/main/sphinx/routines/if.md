# IF

## Synopsis

```text
IF condition
  THEN statements
  [ ELSEIF condition THEN statements ]
  [ ... ]
  [ ELSE statements ]
END IF
```

## Description

The `IF THEN` statement is an optional construct to allow conditional processing
in [SQL routines](/routines/introduction). Each `condition`  following an `IF`
or `ELSEIF` must evaluate to a boolean. The result of processing the expression
must result in a boolean `true` value to process the `statements` in the `THEN`
block. A result of `false` results in skipping the `THEN` block and moving to
evaluate the next `ELSEIF` and `ELSE` blocks in order.

The `ELSEIF` and `ELSE` segments are optional.

## Examples

```sql
FUNCTION simple_if(a bigint)
  RETURNS varchar
  BEGIN
    IF a = 0 THEN
      RETURN 'zero';
    ELSEIF a = 1 THEN
      RETURN 'one';
    ELSE
      RETURN 'more than one or negative';
    END IF;
  END
```

Further examples of varying complexity that cover usage of the `IF` statement in
combination with other statements are available in the [SQL routines examples
documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [Conditional expressions using `IF`](if-expression)
