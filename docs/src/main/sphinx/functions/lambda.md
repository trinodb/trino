(lambda-expressions)=

# Lambda expressions

Lambda expressions are anonymous functions which are passed as
arguments to higher-order SQL functions.

Lambda expressions are written with `->`:

```
x -> x + 1
(x, y) -> x + y
x -> regexp_like(x, 'a+')
x -> x[1] / x[2]
x -> IF(x > 0, x, -x)
x -> COALESCE(x, 0)
x -> CAST(x AS JSON)
x -> x + TRY(1 / 0)
```

## Limitations

Most SQL expressions can be used in a lambda body, with a few exceptions:

- Subqueries are not supported: `x -> 2 + (SELECT 3)`
- Aggregations are not supported: `x -> max(y)`

## Examples

Obtain the squared elements of an array column with {func}`transform`:

```
SELECT numbers,
       transform(numbers, n -> n * n) as squared_numbers
FROM (
    VALUES
        (ARRAY[1, 2]),
        (ARRAY[3, 4]),
        (ARRAY[5, 6, 7])
) AS t(numbers);
```

```text
  numbers  | squared_numbers
-----------+-----------------
 [1, 2]    | [1, 4]
 [3, 4]    | [9, 16]
 [5, 6, 7] | [25, 36, 49]
(3 rows)
```

The function {func}`transform` can be also employed to safely cast the elements
of an array to strings:

```
SELECT transform(prices, n -> TRY_CAST(n AS VARCHAR) || '$') as price_tags
FROM (
    VALUES
        (ARRAY[100, 200]),
        (ARRAY[30, 4])
) AS t(prices);
```

```text
  price_tags
--------------
 [100$, 200$]
 [30$, 4$]
(2 rows)
```

Besides the array column being manipulated,
other columns can be captured as well within the lambda expression.
The following statement provides a showcase of this feature
for calculating the value of the linear function `f(x) = ax + b`
with {func}`transform`:

```
SELECT xvalues,
       a,
       b,
       transform(xvalues, x -> a * x + b) as linear_function_values
FROM (
    VALUES
        (ARRAY[1, 2], 10, 5),
        (ARRAY[3, 4], 4, 2)
) AS t(xvalues, a, b);
```

```text
 xvalues | a  | b | linear_function_values
---------+----+---+------------------------
 [1, 2]  | 10 | 5 | [15, 25]
 [3, 4]  |  4 | 2 | [14, 18]
(2 rows)
```

Find the array elements containing at least one value greater than `100`
with {func}`any_match`:

```
SELECT numbers
FROM (
    VALUES
        (ARRAY[1,NULL,3]),
        (ARRAY[10,20,30]),
        (ARRAY[100,200,300])
) AS t(numbers)
WHERE any_match(numbers, n ->  COALESCE(n, 0) > 100);
-- [100, 200, 300]
```

Capitalize the first word in a string via {func}`regexp_replace`:

```
SELECT regexp_replace('once upon a time ...', '^(\w)(\w*)(\s+.*)$',x -> upper(x[1]) || x[2] || x[3]);
-- Once upon a time ...
```

Lambda expressions can be also applied in aggregation functions.
Following statement is a sample the overly complex calculation of the sum of all elements of a column
by making use of {func}`reduce_agg`:

```
SELECT reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b) sum_values
FROM (
    VALUES (1), (2), (3), (4), (5)
) AS t(value);
-- 15
```
