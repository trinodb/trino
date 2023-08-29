# Example SQL routines


After learning about [SQL routines from the
introduction](/routines/introduction), the following sections show numerous
examples of valid SQL routines. The routines are suitable as [inline
routines](routine-inline) or [catalog routines](routine-catalog), after
adjusting the name and adjusting the example invocations.

The examples combine numerous supported statements. Refer to the specific
statement documentation for further details:

* [](/routines/function) for general SQL routine declaration
* [](/routines/begin) and [](/routines/declare) for routine blocks
* [](/routines/set) for assigning values to variables
* [](/routines/return) for returning routine results
* [](/routines/case) and [](/routines/if) for conditional flows
* [](/routines/loop), [](/routines/repeat), and [](/routines/while) for looping constructs
* [](/routines/iterate) and [](/routines/leave) for flow control

A very simple routine that returns a static value without requiring any input:

```sql
FUNCTION answer()
RETURNS BIGINT
RETURN 42
```

## Inline and catalog routines

A full example of this routine as inline routine and usage in a string
concatenation with a cast:

```sql
WITH
  FUNCTION answer()
  RETURNS BIGINT
  RETURN 42
SELECT 'The answer is ' || CAST(answer() as varchar);
-- The answer is 42
```

Provided the catalog `example` supports routine storage in the `default` schema,
you can use the following:

```sql
USE example.default;
CREATE FUNCTION example.default.answer()
  RETURNS BIGINT
  RETURN 42;
```

With the routine stored in the catalog, you can run the routine multiple times
without repeated definition:

```sql
SELECT example.default.answer() + 1; -- 43
SELECT 'The answer is' || CAST(example.default.answer() as varchar); -- The answer is 42
```

Alternatively, you can configure the SQL environment in the
[](config-properties) to a catalog and schema that support SQL routine storage:

```properties
sql.default-function-catalog=example
sql.default-function-schema=default
```

Now you can manage SQL routines without the full path:

```sql
CREATE FUNCTION answer()
  RETURNS BIGINT
  RETURN 42;
```

SQL routine invocation works without the full path:

```sql
SELECT answer() + 5; -- 47
```

## Declaration examples

The result of calling the routine `answer()` is always identical, so you can
declare it as deterministic, and add some other information:

```sql
FUNCTION answer()
LANGUAGE SQL
DETERMINISTIC
RETURNS BIGINT
COMMENT 'Provide the answer to the question about life, the universe, and everything.'
RETURN 42
```

The comment and other information about the routine is visible in the output of
[](/sql/show-functions).

A simple routine that returns a greeting back to the input string `fullname`
concatenating two strings and the input value:

```sql
FUNCTION hello(fullname VARCHAR)
RETURNS VARCHAR
RETURN 'Hello, ' || fullname || '!'
```

Following is an example invocation:

```sql
SELECT hello('Jane Doe'); -- Hello, Jane Doe!
```

A first example routine, that uses multiple statements in a `BEGIN` block. It
calculates the result of a multiplication of the input integer with `99`. The
`bigint` data type is used for all variables and values. The value of integer
`99` is cast to `bigint` in the default value assignment for the variable `x`.

```sql
FUNCTION times_ninety_nine(a bigint)
RETURNS bigint
BEGIN
  DECLARE x bigint DEFAULT CAST(99 AS bigint);
  RETURN x * a;
END
```

Following is an example invocation:

```sql
SELECT times_ninety_nine(CAST(2 as bigint)); -- 198
```

## Conditional flows

A first example of conditional flow control in a routine using the `CASE`
statement. The simple `bigint` input value is compared to a number of values.

```sql
FUNCTION simple_case(a bigint)
RETURNS varchar
BEGIN
  CASE a
    WHEN 0 THEN RETURN 'zero';
    WHEN 1 THEN RETURN 'one';
    WHEN 10 THEN RETURN 'ten';
    WHEN 20 THEN RETURN 'twenty';
    ELSE RETURN 'other';
  END CASE;
  RETURN NULL;
END
```

Following are a couple of example invocations with result and explanation:

```sql
SELECT simple_case(0); -- zero
SELECT simple_case(1); -- one
SELECT simple_case(-1); -- other (from else clause)
SELECT simple_case(10); -- ten
SELECT simple_case(11); -- other (from else clause)
SELECT simple_case(20); -- twenty
SELECT simple_case(100); -- other (from else clause)
SELECT simple_case(null); -- null .. but really??
```

A second example of a routine with a `CASE` statement, this time with two
parameters, showcasing the importance of the order of the conditions.

```sql
FUNCTION search_case(a bigint, b bigint)
RETURNS varchar
BEGIN
  CASE
    WHEN a = 0 THEN RETURN 'zero';
    WHEN b = 1 THEN RETURN 'one';
    WHEN a = DECIMAL '10.0' THEN RETURN 'ten';
    WHEN b = 20.0E0 THEN RETURN 'twenty';
    ELSE RETURN 'other';
  END CASE;
  RETURN NULL;
END
```

Following are a couple of example invocations with result and explanation:

```sql
SELECT search_case(0,0); -- zero
SELECT search_case(1,1); -- one
SELECT search_case(0,1); -- zero (not one since the second check is never reached)
SELECT search_case(10,1); -- one (not ten since the third check is never reached)
SELECT search_case(10,2); -- ten
SELECT search_case(10,20); -- ten (not twenty)
SELECT search_case(0,20); -- zero (not twenty)
SELECT search_case(3,20); -- twenty
SELECT search_case(3,21); -- other
SELECT simple_case(null,null); -- null .. but really??
```

## Fibonacci example

This routine calculates the `n`-th value in the Fibonacci series, in which each
number is the sum of the two preceding ones. The two initial values are are set
to `1` as the defaults for `a` and `b`. The routine uses an `IF` statement
condition to return `1` for all input values of `2` or less. The `WHILE` block
then starts to calculate each number in the series, starting with `a=1` and
`b=1` and iterates until it reaches the `n`-th position. In each iteration is
sets `a` and `b` for the preceding to values, so it can calculate the sum, and
finally return it. Note that processing the routine takes longer and longer with
higher `n` values, and the result it deterministic.

```sql
FUNCTION fib(n bigint)
RETURNS bigint
BEGIN
  DECLARE a, b bigint DEFAULT 1;
  DECLARE c bigint;
  IF n <= 2 THEN
    RETURN 1;
  END IF;
  WHILE n > 2 DO
    SET n = n - 1;
    SET c = a + b;
    SET a = b;
    SET b = c;
  END WHILE;
  RETURN c;
END
```

Following are a couple of example invocations with result and explanation:

```sql
SELECT fib(-1); -- 1
SELECT fib(0); -- 1
SELECT fib(1); -- 1
SELECT fib(2); -- 1
SELECT fib(3); -- 2
SELECT fib(4); -- 3
SELECT fib(5); -- 5
SELECT fib(6); -- 8
SELECT fib(7); -- 13
SELECT fib(8); -- 21
```

## Labels and loops

This routing uses the `top` label to name the `WHILE` block, and then controls
the flow with conditional statements, `ITERATE`, and `LEAVE`. For the values of
`a=1` and `a=2` in the first two iterations of the loop the `ITERATE` call moves
the flow up to `top` before `b` is ever increased. Then `b` is increased for the
values `a=3`, `a=4`, `a=5`, `a=6`, and `a=7`, resulting in `b=5`. The `LEAVE`
call then causes the exit of the block before a is increased further to `10` and
therefore the result of the routine is `5`.

```sql
FUNCTION labels()
RETURNS bigint
BEGIN
  DECLARE a, b int DEFAULT 0;
  top: WHILE a < 10 DO
    SET a = a + 1;
    IF a < 3 THEN
      ITERATE top;
    END IF;
    SET b = b + 1;
    IF a > 6 THEN
      LEAVE top;
    END IF;
  END WHILE;
  RETURN b;
END
```

This routine implements calculating the `n` to the power of `p` by repeated
multiplication and keeping track of the number of multiplications performed.
Note that this routine does not return the correct `0` for `p=0` since the `top`
block is merely escaped and the value of `n` is returned. The same incorrect
behavior happens for negative values of `p`:

```sql
FUNCTION power(n int, p int)
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

Following are a couple of example invocations with result and explanation:

```sql
SELECT power(2, 2); -- 4
SELECT power(2, 8); -- 256
SELECT power(3, 3); -- 256
SELECT power(3, 0); -- 3, which is wrong
SELECT power(3, -2); -- 3, which is wrong
```

This routine returns `7` as a result of the increase of `b` in the loop from
`a=3` to `a=10`:

```sql
FUNCTION test_repeat_continue()
RETURNS bigint
BEGIN
  DECLARE a int DEFAULT 0;
  DECLARE b int DEFAULT 0;
  top: REPEAT
    SET a = a + 1;
    IF a <= 3 THEN
      ITERATE top;
    END IF;
    SET b = b + 1;
  UNTIL a >= 10
  END REPEAT;
  RETURN b;
END
```

This routine returns `2` and shows that labels can be repeated and label usage
within a block refers to the label of that block:

```sql
FUNCTION test()
RETURNS int
BEGIN
  DECLARE r int DEFAULT 0;
  abc: LOOP
    SET r = r + 1;
    LEAVE abc;
  END LOOP;
  abc: LOOP
    SET r = r + 1;
    LEAVE abc;
  END LOOP;
  RETURN r;
END
```

## Routines and built-in functions

This routine show that multiple data types and built-in functions like
`length()` and `cardinality()` can be used in a routine. The two nested `BEGIN`
blocks also show how variable names are local within these blocks `x`, but the
global `r` from the top-level block can be accessed in the nested blocks:

```sql
FUNCTION test()
RETURNS bigint
BEGIN
  DECLARE r bigint DEFAULT 0;
  BEGIN
    DECLARE x varchar DEFAULT 'hello';
    SET r = r + length(x);
  END;
  BEGIN
    DECLARE x array(int) DEFAULT array[1, 2, 3];
    SET r = r + cardinality(x);
  END;
  RETURN r;
END
```
