# Example SQL UDFs

After learning about [](/udf/sql), the following sections show numerous examples
of valid SQL UDFs. The UDFs are suitable as [](udf-inline) or [](udf-catalog),
after adjusting the name and the example invocations.

The examples combine numerous supported statements. Refer to the specific
statement documentation for further details:

* [](/udf/function) for general UDF declaration
* [](/udf/sql/begin) and [](/udf/sql/declare) for SQL UDF blocks
* [](/udf/sql/set) for assigning values to variables
* [](/udf/sql/return) for returning results
* [](/udf/sql/case) and [](/udf/sql/if) for conditional flows
* [](/udf/sql/loop), [](/udf/sql/repeat), and [](/udf/sql/while) for looping constructs
* [](/udf/sql/iterate) and [](/udf/sql/leave) for flow control

## Inline and catalog UDFs

The following section shows the differences in usage with inline and catalog
UDFs with a simple SQL UDF example. The same pattern applies to all other
following sections.

A very simple SQL UDF that returns a static value without requiring any input:

```sql
FUNCTION answer()
RETURNS BIGINT
RETURN 42
```

A full example of this UDF as inline UDF and usage in a string concatenation
with a cast:

```sql
WITH
  FUNCTION answer()
  RETURNS BIGINT
  RETURN 42
SELECT 'The answer is ' || CAST(answer() as varchar);
-- The answer is 42
```

Provided the catalog `example` supports UDF storage in the `default` schema, you
can use the following:

```sql
CREATE FUNCTION example.default.answer()
  RETURNS BIGINT
  RETURN 42;
```

With the UDF stored in the catalog, you can run the UDF multiple times without
repeated definition:

```sql
SELECT example.default.answer() + 1; -- 43
SELECT 'The answer is ' || CAST(example.default.answer() as varchar); -- The answer is 42
```

Alternatively, you can configure the SQL PATH in the [](config-properties) to a
catalog and schema that support UDF storage:

```properties
sql.default-function-catalog=example
sql.default-function-schema=default
sql.path=example.default
```

Now you can manage UDFs without the full path:

```sql
CREATE FUNCTION answer()
  RETURNS BIGINT
  RETURN 42;
```

UDF invocation works without the full path:

```sql
SELECT answer() + 5; -- 47
```

## Declaration examples

The result of calling the UDF `answer()` is always identical, so you can
declare it as deterministic, and add some other information:

```sql
FUNCTION answer()
LANGUAGE SQL
DETERMINISTIC
RETURNS BIGINT
COMMENT 'Provide the answer to the question about life, the universe, and everything.'
RETURN 42
```

The comment and other information about the UDF is visible in the output of
[](/sql/show-functions).

A simple UDF that returns a greeting back to the input string `fullname`
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

A first example UDF, that uses multiple statements in a `BEGIN` block. It
calculates the result of a multiplication of the input integer with `99`. The
`bigint` data type is used for all variables and values. The value of integer
`99` is cast to `bigint` in the default value assignment for the variable `x`:

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

A first example of conditional flow control in a SQL UDF using the `CASE`
statement. The simple `bigint` input value is compared to a number of values:

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

A second example of a SQL UDF with a `CASE` statement, this time with two
parameters, showcasing the importance of the order of the conditions:

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

This SQL UDF calculates the `n`-th value in the Fibonacci series, in which each
number is the sum of the two preceding ones. The two initial values are set to
`1` as the defaults for `a` and `b`. The UDF uses an `IF` statement condition to
return `1` for all input values of `2` or less. The `WHILE` block then starts to
calculate each number in the series, starting with `a=1` and `b=1` and iterates
until it reaches the `n`-th position. In each iteration it sets `a` and `b` for
the preceding to values, so it can calculate the sum, and finally return it.
Note that processing the UDF takes longer and longer with higher `n` values, and
the result is deterministic:

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

This SQL UDF uses the `top` label to name the `WHILE` block, and then controls
the flow with conditional statements, `ITERATE`, and `LEAVE`. For the values of
`a=1` and `a=2` in the first two iterations of the loop the `ITERATE` call moves
the flow up to `top` before `b` is ever increased. Then `b` is increased for the
values `a=3`, `a=4`, `a=5`, `a=6`, and `a=7`, resulting in `b=5`. The `LEAVE`
call then causes the exit of the block before a is increased further to `10` and
therefore the result of the UDF is `5`:

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

This SQL UDF implements calculating the `n` to the power of `p` by repeated
multiplication and keeping track of the number of multiplications performed.
Note that this SQL UDF does not return the correct `0` for `p=0` since the `top`
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

This SQL UDF returns `7` as a result of the increase of `b` in the loop from
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

This SQL UDF returns `2` and shows that labels can be repeated and label usage
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

## SQL UDFs and built-in functions

This SQL UDF shows that multiple data types and built-in functions like
`length()` and `cardinality()` can be used in a UDF. The two nested `BEGIN`
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

## Optional parameter example

UDFs can invoke other UDFs and other functions. The full signature of a UDF is
composed of the UDF name and parameters, and determines the exact UDF to use.
You can declare multiple UDFs with the same name, but with a different number of
arguments or different argument types. One example use case is to implement an
optional parameter.

The following SQL UDF truncates a string to the specified length including three
dots at the end of the output:

```sql
FUNCTION dots(input varchar, length integer)
RETURNS varchar
BEGIN
  IF length(input) > length THEN
    RETURN substring(input, 1, length-3) || '...';
  END IF;
  RETURN input;
END;
```

Following are example invocations and output:

```sql
SELECT dots('A long string that will be shortened',15);
-- A long strin...
SELECT	dots('A short string',15);
-- A short string
```

If you want to provide a UDF with the same name, but without the parameter
for length, you can create another UDF that invokes the preceding UDF:

```sql
FUNCTION dots(input varchar)
RETURNS varchar
RETURN dots(input, 15);
```

You can now use both UDFs. When the length parameter is omitted, the default
value from the second declaration is used.

```sql
SELECT dots('A long string that will be shortened',15);
-- A long strin...
SELECT dots('A long string that will be shortened');
-- A long strin...
SELECT dots('A long string that will be shortened',20);
-- A long string tha...
```

## Date string parsing example

This example SQL UDF parses a date string of type `VARCHAR` into `TIMESTAMP WITH
TIME ZONE`. Date strings are commonly represented by ISO 8601 standard, such as
`2023-12-01`, `2023-12-01T23`. Date strings are also often represented in the
`YYYYmmdd` and `YYYYmmddHH` format, such as `20230101` and `2023010123`. Hive
tables can use this format to represent day and hourly partitions, for example
`/day=20230101`, `/hour=2023010123`.

This UDF parses date strings in a best-effort fashion and can be used as a
replacement for date string manipulation functions such as `date`, `date_parse`,
`from_iso8601_date`,  and `from_iso8601_timestamp`.

Note that the UDF defaults the time value to `00:00:00.000` and the time
zone to the session time zone:

```sql
FUNCTION from_date_string(date_string VARCHAR)
RETURNS TIMESTAMP WITH TIME ZONE
BEGIN
  IF date_string like '%-%' THEN -- ISO 8601
    RETURN from_iso8601_timestamp(date_string);
  ELSEIF length(date_string) = 8 THEN -- YYYYmmdd
      RETURN date_parse(date_string, '%Y%m%d');
  ELSEIF length(date_string) = 10 THEN -- YYYYmmddHH
      RETURN date_parse(date_string, '%Y%m%d%H');
  END IF;
  RETURN NULL;
END
```

Following are a couple of example invocations with result and explanation:

```sql
SELECT from_date_string('2023-01-01'); -- 2023-01-01 00:00:00.000 UTC (using the ISO 8601 format)
SELECT from_date_string('2023-01-01T23'); -- 2023-01-01 23:00:00.000 UTC (using the ISO 8601 format)
SELECT from_date_string('2023-01-01T23:23:23'); -- 2023-01-01 23:23:23.000 UTC (using the ISO 8601 format)
SELECT from_date_string('20230101'); -- 2023-01-01 00:00:00.000 UTC (using the YYYYmmdd format)
SELECT from_date_string('2023010123'); -- 2023-01-01 23:00:00.000 UTC (using the YYYYmmddHH format)
SELECT from_date_string(NULL); -- NULL (handles NULL string)
SELECT from_date_string('abc'); -- NULL (not matched to any format)
```

## Human readable days

Trino includes a built-in function called {func}`human_readable_seconds` that
formats a number of seconds into a string:

```sql
SELECT human_readable_seconds(134823);
-- 1 day, 13 hours, 27 minutes, 3 seconds
```

The example SQL UDF `hrd` formats a number of days into a human readable text
that provides the approximate number of years and months:

```sql
FUNCTION hrd(d integer)
RETURNS VARCHAR
BEGIN
    DECLARE answer varchar default 'About ';
    DECLARE years real;
    DECLARE months real;
    SET years = truncate(d/365);
    IF years > 0 then
        SET answer = answer || format('%1.0f', years) || ' year';
    END IF;
    IF years > 1 THEN
        SET answer = answer || 's';
    END IF;
    SET d = d - cast( years AS integer) * 365 ;
    SET months = truncate(d / 30);
    IF months > 0 and years > 0 THEN
        SET answer = answer || ' and ';
    END IF;
    IF months > 0 THEN
        set answer = answer || format('%1.0f', months) || ' month';
    END IF;
    IF months > 1 THEN
        SET answer = answer || 's';
    END IF;
    IF years < 1 and months < 1 THEN
        SET answer = 'Less than 1 month';
    END IF;
    RETURN answer;
END;
```

The following examples show the output for a range of values under one month,
under one year, and various larger values:

```sql
SELECT hrd(10); -- Less than 1 month
SELECT hrd(95); -- About 3 months
SELECT hrd(400); -- About 1 year and 1 month
SELECT hrd(369); -- About 1 year
SELECT hrd(800); -- About 2 years and 2 months
SELECT hrd(1100); -- About 3 years
SELECT hrd(5000); -- About 13 years and 8 months
```

Improvements of the SQL UDF could include the following modifications:

* Take into account that one month equals 30.4375 days.
* Take into account that one year equals 365.25 days.
* Add weeks to the output.
* Expand to cover decades, centuries, and millennia.

## Truncating long strings

This example SQL UDF `strtrunc` truncates strings longer than 60 characters,
leaving the first 30 and the last 25 characters, and cutting out extra
characters in the middle:

```sql
FUNCTION strtrunc(input VARCHAR)
RETURNS VARCHAR
RETURN
    CASE WHEN length(input) > 60
    THEN substr(input, 1, 30) || ' ... ' || substr(input, length(input) - 25)
    ELSE input
    END;
```

The preceding declaration is very compact and consists of only one complex
statement with a [`CASE` expression](case-expression) and multiple function
calls. It can therefore define the complete logic in the `RETURN` clause.

The following statement shows the same capability within the SQL UDF itself.
Note the duplicate `RETURN` inside and outside the `CASE` statement and the
required `END CASE;`. The second `RETURN` statement is required, because a SQL
UDF must end with a `RETURN` statement. As a result the `ELSE` clause can be
omitted:

```sql
FUNCTION strtrunc(input VARCHAR)
RETURNS VARCHAR
BEGIN
    CASE WHEN length(input) > 60
    THEN
        RETURN substr(input, 1, 30) || ' ... ' || substr(input, length(input) - 25);
    ELSE
        RETURN input;
    END CASE;
    RETURN input;
END;
```

The next example changes over from a `CASE` to an `IF` statement, and avoids the
duplicate `RETURN`:

```sql
FUNCTION strtrunc(input VARCHAR)
RETURNS VARCHAR
BEGIN
    IF length(input) > 60 THEN
        RETURN substr(input, 1, 30) || ' ... ' || substr(input, length(input) - 25);
    END IF;
    RETURN input;
END;
```

All the preceding examples create the same output. Following is an example query
which generates long strings to truncate:

```sql
WITH
data AS (
    SELECT substring('strtrunc truncates strings longer than 60 characters,
     leaving the prefix and suffix visible', 1, s.num) AS value
    FROM table(sequence(start=>40, stop=>80, step=>5)) AS s(num)
)
SELECT
    data.value
  , strtrunc(data.value) AS truncated
FROM data
ORDER BY data.value;
```

The preceding query produces the following output with all variants of the SQL
UDF:

```
                                      value                                       |                           truncated
----------------------------------------------------------------------------------+---------------------------------------------------------------
 strtrunc truncates strings longer than 6                                         | strtrunc truncates strings longer than 6
 strtrunc truncates strings longer than 60 cha                                    | strtrunc truncates strings longer than 60 cha
 strtrunc truncates strings longer than 60 characte                               | strtrunc truncates strings longer than 60 characte
 strtrunc truncates strings longer than 60 characters, l                          | strtrunc truncates strings longer than 60 characters, l
 strtrunc truncates strings longer than 60 characters, leavin                     | strtrunc truncates strings longer than 60 characters, leavin
 strtrunc truncates strings longer than 60 characters, leaving the                | strtrunc truncates strings lon ... 60 characters, leaving the
 strtrunc truncates strings longer than 60 characters, leaving the pref           | strtrunc truncates strings lon ... aracters, leaving the pref
 strtrunc truncates strings longer than 60 characters, leaving the prefix an      | strtrunc truncates strings lon ... ers, leaving the prefix an
 strtrunc truncates strings longer than 60 characters, leaving the prefix and suf | strtrunc truncates strings lon ... leaving the prefix and suf
```

A possible improvement is to introduce parameters for the total length.

## Formatting bytes

Trino includes a built-in `format_number()` function. However, it is using units
that do not work well with bytes. The following `format_data_size` SQL UDF can
format large values of bytes into a human readable string:

```sql
FUNCTION format_data_size(input BIGINT)
RETURNS VARCHAR
  BEGIN
    DECLARE value DOUBLE DEFAULT CAST(input AS DOUBLE);
    DECLARE result BIGINT;
    DECLARE base INT DEFAULT 1024;
    DECLARE unit VARCHAR DEFAULT 'B';
    DECLARE format VARCHAR;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'kB';
    END IF;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'MB';
    END IF;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'GB';
    END IF;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'TB';
    END IF;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'PB';
    END IF;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'EB';
    END IF;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'ZB';
    END IF;
    IF abs(value) >= base THEN
      SET value = value / base;
      SET unit = 'YB';
    END IF;
    IF abs(value) < 10 THEN
      SET format = '%.2f';
    ELSEIF abs(value) < 100 THEN
      SET format = '%.1f';
    ELSE
      SET format = '%.0f';
    END IF;
    RETURN format(format, value) || unit;
  END;
```

Below is a query that shows how it formats a wide range of values:

```sql
WITH
data AS (
    SELECT CAST(pow(10, s.p) AS BIGINT) AS num
    FROM table(sequence(start=>1, stop=>18)) AS s(p)
    UNION ALL
    SELECT -CAST(pow(10, s.p) AS BIGINT) AS num
    FROM table(sequence(start=>1, stop=>18)) AS s(p)
)
SELECT
    data.num
  , format_data_size(data.num) AS formatted
FROM data
ORDER BY data.num;
```

The preceding query produces the following output:

```
         num          | formatted
----------------------+-----------
 -1000000000000000000 | -888PB
  -100000000000000000 | -88.8PB
   -10000000000000000 | -8.88PB
    -1000000000000000 | -909TB
     -100000000000000 | -90.9TB
      -10000000000000 | -9.09TB
       -1000000000000 | -931GB
        -100000000000 | -93.1GB
         -10000000000 | -9.31GB
          -1000000000 | -954MB
           -100000000 | -95.4MB
            -10000000 | -9.54MB
             -1000000 | -977kB
              -100000 | -97.7kB
               -10000 | -9.77kB
                -1000 | -1000B
                 -100 | -100B
                  -10 | -10.0B
                    0 | 0.00B
                   10 | 10.0B
                  100 | 100B
                 1000 | 1000B
                10000 | 9.77kB
               100000 | 97.7kB
              1000000 | 977kB
             10000000 | 9.54MB
            100000000 | 95.4MB
           1000000000 | 954MB
          10000000000 | 9.31GB
         100000000000 | 93.1GB
        1000000000000 | 931GB
       10000000000000 | 9.09TB
      100000000000000 | 90.9TB
     1000000000000000 | 909TB
    10000000000000000 | 8.88PB
   100000000000000000 | 88.8PB
  1000000000000000000 | 888PB
```


## Charts

Trino already has a built-in `bar()` [color function](/functions/color), but it
is using ANSI escape codes to output colors, and thus is only usable for
displaying results in a terminal. The following example shows a similar SQL UDF
that only uses ASCII characters:

```sql
FUNCTION ascii_bar(value DOUBLE)
RETURNS VARCHAR
BEGIN
  DECLARE max_width DOUBLE DEFAULT 40.0;
  RETURN array_join(
    repeat('█',
        greatest(0, CAST(floor(max_width * value) AS integer) - 1)), '')
        || ARRAY[' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█']
        [cast((value % (cast(1 as double) / max_width)) * max_width * 8 + 1 as int)];
END;
```

It can be used to visualize a value:

```sql
WITH
data AS (
    SELECT
        cast(s.num as double) / 100.0 AS x,
        sin(cast(s.num as double) / 100.0) AS y
    FROM table(sequence(start=>0, stop=>314, step=>10)) AS s(num)
)
SELECT
    data.x,
    round(data.y, 4) AS y,
    ascii_bar(data.y) AS chart
FROM data
ORDER BY data.x;
```

The preceding query produces the following output:

```text
  x  |   y    |                  chart
-----+--------+-----------------------------------------
 0.0 |    0.0 |
 0.1 | 0.0998 | ███
 0.2 | 0.1987 | ███████
 0.3 | 0.2955 | ██████████▉
 0.4 | 0.3894 | ██████████████▋
 0.5 | 0.4794 | ██████████████████▏
 0.6 | 0.5646 | █████████████████████▋
 0.7 | 0.6442 | ████████████████████████▊
 0.8 | 0.7174 | ███████████████████████████▊
 0.9 | 0.7833 | ██████████████████████████████▍
 1.0 | 0.8415 | ████████████████████████████████▋
 1.1 | 0.8912 | ██████████████████████████████████▋
 1.2 |  0.932 | ████████████████████████████████████▎
 1.3 | 0.9636 | █████████████████████████████████████▌
 1.4 | 0.9854 | ██████████████████████████████████████▍
 1.5 | 0.9975 | ██████████████████████████████████████▉
 1.6 | 0.9996 | ███████████████████████████████████████
 1.7 | 0.9917 | ██████████████████████████████████████▋
 1.8 | 0.9738 | ██████████████████████████████████████
 1.9 | 0.9463 | ████████████████████████████████████▉
 2.0 | 0.9093 | ███████████████████████████████████▍
 2.1 | 0.8632 | █████████████████████████████████▌
 2.2 | 0.8085 | ███████████████████████████████▍
 2.3 | 0.7457 | ████████████████████████████▉
 2.4 | 0.6755 | ██████████████████████████
 2.5 | 0.5985 | ███████████████████████
 2.6 | 0.5155 | ███████████████████▋
 2.7 | 0.4274 | ████████████████▏
 2.8 |  0.335 | ████████████▍
 2.9 | 0.2392 | ████████▋
 3.0 | 0.1411 | ████▋
 3.1 | 0.0416 | ▋
```

It is also possible to draw more compacted charts. Following is a SQL UDF
drawing vertical bars:

```sql
FUNCTION vertical_bar(value DOUBLE)
RETURNS VARCHAR
RETURN ARRAY[' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'][cast(value * 8 + 1 as int)];
```

It can be used to draw a distribution of values, in a single column:

```sql
WITH
measurements(sensor_id, recorded_at, value) AS (
    VALUES
        ('A', date '2023-01-01', 5.0)
      , ('A', date '2023-01-03', 7.0)
      , ('A', date '2023-01-04', 15.0)
      , ('A', date '2023-01-05', 14.0)
      , ('A', date '2023-01-08', 10.0)
      , ('A', date '2023-01-09', 1.0)
      , ('A', date '2023-01-10', 7.0)
      , ('A', date '2023-01-11', 8.0)
      , ('B', date '2023-01-03', 2.0)
      , ('B', date '2023-01-04', 3.0)
      , ('B', date '2023-01-05', 2.5)
      , ('B', date '2023-01-07', 2.75)
      , ('B', date '2023-01-09', 4.0)
      , ('B', date '2023-01-10', 1.5)
      , ('B', date '2023-01-11', 1.0)
),
days AS (
    SELECT date_add('day', s.num, date '2023-01-01') AS day
    -- table function arguments need to be constant but range could be calculated
    -- using: SELECT date_diff('day', max(recorded_at), min(recorded_at)) FROM measurements
    FROM table(sequence(start=>0, stop=>10)) AS s(num)
),
sensors(id) AS (VALUES ('A'), ('B')),
normalized AS (
    SELECT
        sensors.id AS sensor_id,
        days.day,
        value,
        value / max(value) OVER (PARTITION BY sensor_id) AS normalized
    FROM days
    CROSS JOIN sensors
    LEFT JOIN measurements m ON day = recorded_at AND m.sensor_id = sensors.id
)
SELECT
    sensor_id,
    min(day) AS start,
    max(day) AS stop,
    count(value) AS num_values,
    min(value) AS min_value,
    max(value) AS max_value,
    avg(value) AS avg_value,
    array_join(array_agg(coalesce(vertical_bar(normalized), ' ') ORDER BY day),
     '') AS distribution
FROM normalized
WHERE sensor_id IS NOT NULL
GROUP BY sensor_id
ORDER BY sensor_id;
```

The preceding query produces the following output:

```text
 sensor_id |   start    |    stop    | num_values | min_value | max_value | avg_value | distribution
-----------+------------+------------+------------+-----------+-----------+-----------+--------------
 A         | 2023-01-01 | 2023-01-11 |          8 |      1.00 |     15.00 |      8.38 | ▃ ▄█▇  ▅▁▄▄
 B         | 2023-01-01 | 2023-01-11 |          7 |      1.00 |      4.00 |      2.39 |   ▄▆▅ ▆ █▃▂
```

## Top-N

Trino already has a built-in [aggregate function](/functions/aggregate) called
`approx_most_frequent()` that can calculate the most frequently occurring
values. It returns a map with values as keys and number of occurrences as
values. Maps are not ordered, so when displayed, the entries can change places
on subsequent runs of the same query, and readers must still compare all
frequencies to find the one most frequent value. The following is a SQL UDF that
returns ordered results as a string:

```sql
FUNCTION format_topn(input map<varchar, bigint>)
RETURNS VARCHAR
NOT DETERMINISTIC
BEGIN
  DECLARE freq_separator VARCHAR DEFAULT '=';
  DECLARE entry_separator VARCHAR DEFAULT ', ';
  RETURN array_join(transform(
    reverse(array_sort(transform(
      transform(
        map_entries(input),
          r -> cast(r AS row(key varchar, value bigint))
      ),
      r -> cast(row(r.value, r.key) AS row(value bigint, key varchar)))
    )),
    r -> r.key || freq_separator || cast(r.value as varchar)),
    entry_separator);
END;
```

Following is an example query to count generated strings:

```sql
WITH
data AS (
    SELECT lpad('', 3, chr(65+(s.num / 3))) AS value
    FROM table(sequence(start=>1, stop=>10)) AS s(num)
),
aggregated AS (
    SELECT
        array_agg(data.value ORDER BY data.value) AS all_values,
        approx_most_frequent(3, data.value, 1000) AS top3
    FROM data
)
SELECT
    a.all_values,
    a.top3,
    format_topn(a.top3) AS top3_formatted
FROM aggregated a;
```

The preceding query produces the following result:

```text
                     all_values                     |         top3          |    top3_formatted
----------------------------------------------------+-----------------------+---------------------
 [AAA, AAA, BBB, BBB, BBB, CCC, CCC, CCC, DDD, DDD] | {AAA=2, CCC=3, BBB=3} | CCC=3, BBB=3, AAA=2
```
