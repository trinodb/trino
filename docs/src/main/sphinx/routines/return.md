# RETURN

## Synopsis

```text
RETURN expression
```

## Description

Provide the value from a [SQL routines](/routines/introduction) to the caller.
The value is the result of evaluating the expression. It can be a static value,
a declared variable or a more complex expression.

## Examples

The following examples return a static value, the result of an expression, and
the value of the variable x:

```sql
RETURN 42;
RETURN 6 * 7;
RETURN x;
```

Further examples of varying complexity that cover usage of the `RETURN`
statement in combination with other statements are available in the [SQL
routines examples documentation](/routines/examples).

All routines must contain a `RETURN` statement at the end of the top-level
block in the `FUNCTION` declaration, even if it's unreachable.

## See also

* [](/routines/introduction)
* [](/routines/function)
