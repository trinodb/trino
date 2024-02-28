# LEAVE

## Synopsis

```text
LEAVE label
```

## Description

The `LEAVE` statement allows processing of blocks in [SQL
routines](/routines/introduction) to move out of a specified context. Contexts
are defined by a [`label`](routine-label). If no label is found, the functions
fails with an error message.

## Examples

The following function includes a `LOOP` labelled `top`. The conditional `IF`
statement inside the loop can cause the exit from processing the loop when the
value for the parameter `p` is 1 or less. This can be the case if the value is
passed in as 1 or less or after a number of iterations through the loop.

```sql
FUNCTION my_pow(n int, p int)
RETURNS int
BEGIN
  DECLARE r int DEFAULT n;
  top: LOOP
    IF p <= 1 THEN
      LEAVE top;
    END IF;
    SET r = r * n;
    SET p = p - 1;
  END LOOP;
  RETURN r;
END
```

Further examples of varying complexity that cover usage of the `LEAVE` statement
in combination with other statements are available in the [SQL routines examples
documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [](/routines/iterate)
