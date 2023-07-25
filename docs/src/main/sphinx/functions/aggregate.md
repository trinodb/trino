# Aggregate functions

Aggregate functions operate on a set of values to compute a single result.

Except for {func}`count`, {func}`count_if`, {func}`max_by`, {func}`min_by` and
{func}`approx_distinct`, all of these aggregate functions ignore null values
and return null for no input rows or when all values are null. For example,
{func}`sum` returns null rather than zero and {func}`avg` does not include null
values in the count. The `coalesce` function can be used to convert null into
zero.

(aggregate-function-ordering-during-aggregation)=

## Ordering during aggregation

Some aggregate functions such as {func}`array_agg` produce different results
depending on the order of input values. This ordering can be specified by writing
an {ref}`order-by-clause` within the aggregate function:

```
array_agg(x ORDER BY y DESC)
array_agg(x ORDER BY x, y, z)
```

(aggregate-function-filtering-during-aggregation)=

## Filtering during aggregation

The `FILTER` keyword can be used to remove rows from aggregation processing
with a condition expressed using a `WHERE` clause. This is evaluated for each
row before it is used in the aggregation and is supported for all aggregate
functions.

```text
aggregate_function(...) FILTER (WHERE <condition>)
```

A common and very useful example is to use `FILTER` to remove nulls from
consideration when using `array_agg`:

```
SELECT array_agg(name) FILTER (WHERE name IS NOT NULL)
FROM region;
```

As another example, imagine you want to add a condition on the count for Iris
flowers, modifying the following query:

```
SELECT species,
       count(*) AS count
FROM iris
GROUP BY species;
```

```text
species    | count
-----------+-------
setosa     |   50
virginica  |   50
versicolor |   50
```

If you just use a normal `WHERE` statement you lose information:

```
SELECT species,
    count(*) AS count
FROM iris
WHERE petal_length_cm > 4
GROUP BY species;
```

```text
species    | count
-----------+-------
virginica  |   50
versicolor |   34
```

Using a filter you retain all information:

```
SELECT species,
       count(*) FILTER (where petal_length_cm > 4) AS count
FROM iris
GROUP BY species;
```

```text
species    | count
-----------+-------
virginica  |   50
setosa     |    0
versicolor |   34
```

## General aggregate functions

:::{function} any_value(x) -> [same as input]
Returns an arbitrary non-null value `x`, if one exists. `x` can be any
valid expression. This allows you to return values from columns that are not
directly part of the aggregation, inluding expressions using these columns,
in a query.

For example, the following query returns the customer name from the `name`
column, and returns the sum of all total prices as customer spend. The
aggregation however uses the rows grouped by the customer identifier
`custkey` a required, since only that column is guaranteed to be unique:

```
SELECT sum(o.totalprice) as spend,
    any_value(c.name)
FROM tpch.tiny.orders o
JOIN tpch.tiny.customer c
ON o.custkey  = c.custkey
GROUP BY c.custkey;
ORDER BY spend;
```
:::

:::{function} arbitrary(x) -> [same as input]
Returns an arbitrary non-null value of `x`, if one exists. Identical to
{func}`any_value`.
:::

:::{function} array_agg(x) -> array<[same as input]>
Returns an array created from the input `x` elements.
:::

:::{function} avg(x) -> double
Returns the average (arithmetic mean) of all input values.
:::

:::{function} avg(time interval type) -> time interval type
:noindex: true

Returns the average interval length of all input values.
:::

:::{function} bool_and(boolean) -> boolean
Returns `TRUE` if every input value is `TRUE`, otherwise `FALSE`.
:::

:::{function} bool_or(boolean) -> boolean
Returns `TRUE` if any input value is `TRUE`, otherwise `FALSE`.
:::

:::{function} checksum(x) -> varbinary
Returns an order-insensitive checksum of the given values.
:::

:::{function} count(*) -> bigint
Returns the number of input rows.
:::

:::{function} count(x) -> bigint
:noindex: true

Returns the number of non-null input values.
:::

:::{function} count_if(x) -> bigint
Returns the number of `TRUE` input values.
This function is equivalent to `count(CASE WHEN x THEN 1 END)`.
:::

:::{function} every(boolean) -> boolean
This is an alias for {func}`bool_and`.
:::

:::{function} geometric_mean(x) -> double
Returns the geometric mean of all input values.
:::

:::{function} listagg(x, separator) -> varchar
Returns the concatenated input values, separated by the `separator` string.

Synopsis:

```
LISTAGG( expression [, separator] [ON OVERFLOW overflow_behaviour])
    WITHIN GROUP (ORDER BY sort_item, [...])
```

If `separator` is not specified, the empty string will be used as `separator`.

In its simplest form the function looks like:

```
SELECT listagg(value, ',') WITHIN GROUP (ORDER BY value) csv_value
FROM (VALUES 'a', 'c', 'b') t(value);
```

and results in:

```
csv_value
-----------
'a,b,c'
```

The overflow behaviour is by default to throw an error in case that the length of the output
of the function exceeds `1048576` bytes:

```
SELECT listagg(value, ',' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY value) csv_value
FROM (VALUES 'a', 'b', 'c') t(value);
```

There exists also the possibility to truncate the output `WITH COUNT` or `WITHOUT COUNT`
of omitted non-null values in case that the length of the output of the
function exceeds `1048576` bytes:

```
SELECT LISTAGG(value, ',' ON OVERFLOW TRUNCATE '.....' WITH COUNT) WITHIN GROUP (ORDER BY value)
FROM (VALUES 'a', 'b', 'c') t(value);
```

If not specified, the truncation filler string is by default `'...'`.

This aggregation function can be also used in a scenario involving grouping:

```
SELECT id, LISTAGG(value, ',') WITHIN GROUP (ORDER BY o) csv_value
FROM (VALUES
    (100, 1, 'a'),
    (200, 3, 'c'),
    (200, 2, 'b')
) t(id, o, value)
GROUP BY id
ORDER BY id;
```

results in:

```text
 id  | csv_value
-----+-----------
 100 | a
 200 | b,c
```

The current implementation of `LISTAGG` function does not support window frames.
:::

:::{function} max(x) -> [same as input]
Returns the maximum value of all input values.
:::

:::{function} max(x, n) -> array<[same as x]>
:noindex: true

Returns `n` largest values of all input values of `x`.
:::

:::{function} max_by(x, y) -> [same as x]
Returns the value of `x` associated with the maximum value of `y` over all input values.
:::

:::{function} max_by(x, y, n) -> array<[same as x]>
:noindex: true

Returns `n` values of `x` associated with the `n` largest of all input values of `y`
in descending order of `y`.
:::

:::{function} min(x) -> [same as input]
Returns the minimum value of all input values.
:::

:::{function} min(x, n) -> array<[same as x]>
:noindex: true

Returns `n` smallest values of all input values of `x`.
:::

:::{function} min_by(x, y) -> [same as x]
Returns the value of `x` associated with the minimum value of `y` over all input values.
:::

:::{function} min_by(x, y, n) -> array<[same as x]>
:noindex: true

Returns `n` values of `x` associated with the `n` smallest of all input values of `y`
in ascending order of `y`.
:::

:::{function} sum(x) -> [same as input]
Returns the sum of all input values.
:::

## Bitwise aggregate functions

:::{function} bitwise_and_agg(x) -> bigint
Returns the bitwise AND of all input values in 2's complement representation.
:::

:::{function} bitwise_or_agg(x) -> bigint
Returns the bitwise OR of all input values in 2's complement representation.
:::

## Map aggregate functions

:::{function} histogram(x) -> map<K,bigint>
Returns a map containing the count of the number of times each input value occurs.
:::

:::{function} map_agg(key, value) -> map<K,V>
Returns a map created from the input `key` / `value` pairs.
:::

:::{function} map_union(x(K,V)) -> map<K,V>
Returns the union of all the input maps. If a key is found in multiple
input maps, that key's value in the resulting map comes from an arbitrary input map.

For example, take the following histogram function that creates multiple maps from the Iris dataset:

```
SELECT histogram(floor(petal_length_cm)) petal_data
FROM memory.default.iris
GROUP BY species;

        petal_data
-- {4.0=6, 5.0=33, 6.0=11}
-- {4.0=37, 5.0=2, 3.0=11}
-- {1.0=50}
```

You can combine these maps using `map_union`:

```
SELECT map_union(petal_data) petal_data_union
FROM (
       SELECT histogram(floor(petal_length_cm)) petal_data
       FROM memory.default.iris
       GROUP BY species
       );

             petal_data_union
--{4.0=6, 5.0=2, 6.0=11, 1.0=50, 3.0=11}
```
:::

:::{function} multimap_agg(key, value) -> map<K,array(V)>
Returns a multimap created from the input `key` / `value` pairs.
Each key can be associated with multiple values.
:::

## Approximate aggregate functions

:::{function} approx_distinct(x) -> bigint
Returns the approximate number of distinct input values.
This function provides an approximation of `count(DISTINCT x)`.
Zero is returned if all input values are null.

This function should produce a standard error of 2.3%, which is the
standard deviation of the (approximately normal) error distribution over
all possible sets. It does not guarantee an upper bound on the error for
any specific input set.
:::

:::{function} approx_distinct(x, e) -> bigint
:noindex: true

Returns the approximate number of distinct input values.
This function provides an approximation of `count(DISTINCT x)`.
Zero is returned if all input values are null.

This function should produce a standard error of no more than `e`, which
is the standard deviation of the (approximately normal) error distribution
over all possible sets. It does not guarantee an upper bound on the error
for any specific input set. The current implementation of this function
requires that `e` be in the range of `[0.0040625, 0.26000]`.
:::

:::{function} approx_most_frequent(buckets, value, capacity) -> map<[same as value], bigint>
Computes the top frequent values up to `buckets` elements approximately.
Approximate estimation of the function enables us to pick up the frequent
values with less memory. Larger `capacity` improves the accuracy of
underlying algorithm with sacrificing the memory capacity. The returned
value is a map containing the top elements with corresponding estimated
frequency.

The error of the function depends on the permutation of the values and its
cardinality. We can set the capacity same as the cardinality of the
underlying data to achieve the least error.

`buckets` and `capacity` must be `bigint`. `value` can be numeric
or string type.

The function uses the stream summary data structure proposed in the paper
[Efficient Computation of Frequent and Top-k Elements in Data Streams](https://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf)
by A. Metwalley, D. Agrawl and A. Abbadi.
:::

:::{function} approx_percentile(x, percentage) -> [same as x]
Returns the approximate percentile for all input values of `x` at the
given `percentage`. The value of `percentage` must be between zero and
one and must be constant for all input rows.
:::

:::{function} approx_percentile(x, percentages) -> array<[same as x]>
:noindex: true

Returns the approximate percentile for all input values of `x` at each of
the specified percentages. Each element of the `percentages` array must be
between zero and one, and the array must be constant for all input rows.
:::

:::{function} approx_percentile(x, w, percentage) -> [same as x]
:noindex: true

Returns the approximate weighed percentile for all input values of `x`
using the per-item weight `w` at the percentage `percentage`. Weights must be
greater or equal to 1. Integer-value weights can be thought of as a replication
count for the value `x` in the percentile set. The value of `percentage` must be
between zero and one and must be constant for all input rows.
:::

:::{function} approx_percentile(x, w, percentages) -> array<[same as x]>
:noindex: true

Returns the approximate weighed percentile for all input values of `x`
using the per-item weight `w` at each of the given percentages specified
in the array. Weights must be greater or equal to 1. Integer-value weights can
be thought of as a replication count for the value `x` in the percentile
set. Each element of the `percentages` array must be between zero and one, and the array
must be constant for all input rows.
:::

:::{function} approx_set(x) -> HyperLogLog
:noindex: true

See {doc}`hyperloglog`.
:::

:::{function} merge(x) -> HyperLogLog
:noindex: true

See {doc}`hyperloglog`.
:::

:::{function} merge(qdigest(T)) -> qdigest(T)
:noindex: true

See {doc}`qdigest`.
:::

:::{function} merge(tdigest) -> tdigest
:noindex: true

See {doc}`tdigest`.
:::

:::{function} numeric_histogram(buckets, value) -> map<double, double>
:noindex: true

Computes an approximate histogram with up to `buckets` number of buckets
for all `value`s. This function is equivalent to the variant of
{func}`numeric_histogram` that takes a `weight`, with a per-item weight of `1`.
:::

:::{function} numeric_histogram(buckets, value, weight) -> map<double, double>
Computes an approximate histogram with up to `buckets` number of buckets
for all `value`s with a per-item weight of `weight`. The algorithm
is based loosely on:

```text
Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
J. Machine Learning Research 11 (2010), pp. 849--872.
```

`buckets` must be a `bigint`. `value` and `weight` must be numeric.
:::

:::{function} qdigest_agg(x) -> qdigest([same as x])
:noindex: true

See {doc}`qdigest`.
:::

:::{function} qdigest_agg(x, w) -> qdigest([same as x])
:noindex: true

See {doc}`qdigest`.
:::

:::{function} qdigest_agg(x, w, accuracy) -> qdigest([same as x])
:noindex: true

See {doc}`qdigest`.
:::

:::{function} tdigest_agg(x) -> tdigest
:noindex: true

See {doc}`tdigest`.
:::

:::{function} tdigest_agg(x, w) -> tdigest
:noindex: true

See {doc}`tdigest`.
:::

## Statistical aggregate functions

:::{function} corr(y, x) -> double
Returns correlation coefficient of input values.
:::

:::{function} covar_pop(y, x) -> double
Returns the population covariance of input values.
:::

:::{function} covar_samp(y, x) -> double
Returns the sample covariance of input values.
:::

:::{function} kurtosis(x) -> double
Returns the excess kurtosis of all input values. Unbiased estimate using
the following expression:

```text
kurtosis(x) = n(n+1)/((n-1)(n-2)(n-3))sum[(x_i-mean)^4]/stddev(x)^4-3(n-1)^2/((n-2)(n-3))
```
:::

:::{function} regr_intercept(y, x) -> double
Returns linear regression intercept of input values. `y` is the dependent
value. `x` is the independent value.
:::

:::{function} regr_slope(y, x) -> double
Returns linear regression slope of input values. `y` is the dependent
value. `x` is the independent value.
:::

:::{function} skewness(x) -> double
Returns the Fisher’s moment coefficient of [skewness](https://wikipedia.org/wiki/Skewness) of all input values.
:::

:::{function} stddev(x) -> double
This is an alias for {func}`stddev_samp`.
:::

:::{function} stddev_pop(x) -> double
Returns the population standard deviation of all input values.
:::

:::{function} stddev_samp(x) -> double
Returns the sample standard deviation of all input values.
:::

:::{function} variance(x) -> double
This is an alias for {func}`var_samp`.
:::

:::{function} var_pop(x) -> double
Returns the population variance of all input values.
:::

:::{function} var_samp(x) -> double
Returns the sample variance of all input values.
:::

## Lambda aggregate functions

:::{function} reduce_agg(inputValue T, initialState S, inputFunction(S, T, S), combineFunction(S, S, S)) -> S
Reduces all input values into a single value. `inputFunction` will be invoked
for each non-null input value. In addition to taking the input value, `inputFunction`
takes the current state, initially `initialState`, and returns the new state.
`combineFunction` will be invoked to combine two states into a new state.
The final state is returned:

```
SELECT id, reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b)
FROM (
    VALUES
        (1, 3),
        (1, 4),
        (1, 5),
        (2, 6),
        (2, 7)
) AS t(id, value)
GROUP BY id;
-- (1, 12)
-- (2, 13)

SELECT id, reduce_agg(value, 1, (a, b) -> a * b, (a, b) -> a * b)
FROM (
    VALUES
        (1, 3),
        (1, 4),
        (1, 5),
        (2, 6),
        (2, 7)
) AS t(id, value)
GROUP BY id;
-- (1, 60)
-- (2, 42)
```

The state type must be a boolean, integer, floating-point, or date/time/interval.
:::
