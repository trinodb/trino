# Window functions

Window functions perform calculations across rows of the query result.
They run after the `HAVING` clause but before the `ORDER BY` clause.
Invoking a window function requires special syntax using the `OVER`
clause to specify the window.
For example, the following query ranks orders for each clerk by price:

```
SELECT orderkey, clerk, totalprice,
       rank() OVER (PARTITION BY clerk
                    ORDER BY totalprice DESC) AS rnk
FROM orders
ORDER BY clerk, rnk
```

The window can be specified in two ways (see {ref}`window-clause`):

- By a reference to a named window specification defined in the `WINDOW` clause,
- By an in-line window specification which allows to define window components
  as well as refer to the window components pre-defined in the `WINDOW` clause.

## Aggregate functions

All {doc}`aggregate` can be used as window functions by adding the `OVER`
clause. The aggregate function is computed for each row over the rows within
the current row's window frame.

For example, the following query produces a rolling sum of order prices
by day for each clerk:

```
SELECT clerk, orderdate, orderkey, totalprice,
       sum(totalprice) OVER (PARTITION BY clerk
                             ORDER BY orderdate) AS rolling_sum
FROM orders
ORDER BY clerk, orderdate, orderkey
```

## Ranking functions

:::{function} cume_dist() -> bigint
Returns the cumulative distribution of a value in a group of values.
The result is the number of rows preceding or peer with the row in the
window ordering of the window partition divided by the total number of
rows in the window partition. Thus, any tie values in the ordering will
evaluate to the same distribution value.
:::

:::{function} dense_rank() -> bigint
Returns the rank of a value in a group of values. This is similar to
{func}`rank`, except that tie values do not produce gaps in the sequence.
:::

:::{function} ntile(n) -> bigint
Divides the rows for each window partition into `n` buckets ranging
from `1` to at most `n`. Bucket values will differ by at most `1`.
If the number of rows in the partition does not divide evenly into the
number of buckets, then the remainder values are distributed one per
bucket, starting with the first bucket.

For example, with `6` rows and `4` buckets, the bucket values would
be as follows: `1` `1` `2` `2` `3` `4`
:::

:::{function} percent_rank() -> double
Returns the percentage ranking of a value in group of values. The result
is `(r - 1) / (n - 1)` where `r` is the {func}`rank` of the row and
`n` is the total number of rows in the window partition.
:::

:::{function} rank() -> bigint
Returns the rank of a value in a group of values. The rank is one plus
the number of rows preceding the row that are not peer with the row.
Thus, tie values in the ordering will produce gaps in the sequence.
The ranking is performed for each window partition.
:::

:::{function} row_number() -> bigint
Returns a unique, sequential number for each row, starting with one,
according to the ordering of rows within the window partition.
:::

## Value functions

By default, null values are respected. If `IGNORE NULLS` is specified, all rows where
`x` is null are excluded from the calculation. If `IGNORE NULLS` is specified and `x`
is null for all rows, the `default_value` is returned, or if it is not specified,
`null` is returned.

:::{function} first_value(x) -> [same as input]
Returns the first value of the window.
:::

:::{function} last_value(x) -> [same as input]
Returns the last value of the window.
:::

:::{function} nth_value(x, offset) -> [same as input]
Returns the value at the specified offset from the beginning of the window.
Offsets start at `1`. The offset can be any scalar
expression.  If the offset is null or greater than the number of values in
the window, `null` is returned.  It is an error for the offset to be zero or
negative.
:::

:::{function} lead(x[, offset [, default_value]]) -> [same as input]
Returns the value at `offset` rows after the current row in the window partition.
Offsets start at `0`, which is the current row. The
offset can be any scalar expression.  The default `offset` is `1`. If the
offset is null, `null` is returned. If the offset refers to a row that is not
within the partition, the `default_value` is returned, or if it is not specified
`null` is returned.
The {func}`lead` function requires that the window ordering be specified.
Window frame must not be specified.
:::

:::{function} lag(x[, offset [, default_value]]) -> [same as input]
Returns the value at `offset` rows before the current row in the window partition.
Offsets start at `0`, which is the current row. The
offset can be any scalar expression.  The default `offset` is `1`. If the
offset is null, `null` is returned. If the offset refers to a row that is not
within the partition, the `default_value` is returned, or if it is not specified
`null` is returned.
The {func}`lag` function requires that the window ordering be specified.
Window frame must not be specified.
:::
