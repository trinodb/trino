# HyperLogLog functions

Trino implements the {func}`approx_distinct` function using the
[HyperLogLog](https://wikipedia.org/wiki/HyperLogLog) data structure.

## Data structures

Trino implements HyperLogLog data sketches as a set of 32-bit buckets which
store a *maximum hash*. They can be stored sparsely (as a map from bucket ID
to bucket), or densely (as a contiguous memory block). The HyperLogLog data
structure starts as the sparse representation, switching to dense when it is
more efficient. The P4HyperLogLog structure is initialized densely and
remains dense for its lifetime.

{ref}`hyperloglog-type` implicitly casts to {ref}`p4hyperloglog-type`,
while one can explicitly cast `HyperLogLog` to `P4HyperLogLog`:

```
cast(hll AS P4HyperLogLog)
```

## Serialization

Data sketches can be serialized to and deserialized from `varbinary`. This
allows them to be stored for later use.  Combined with the ability to merge
multiple sketches, this allows one to calculate {func}`approx_distinct` of the
elements of a partition of a query, then for the entirety of a query with very
little cost.

For example, calculating the `HyperLogLog` for daily unique users will allow
weekly or monthly unique users to be calculated incrementally by combining the
dailies. This is similar to computing weekly revenue by summing daily revenue.
Uses of {func}`approx_distinct` with `GROUPING SETS` can be converted to use
`HyperLogLog`.  Examples:

```
CREATE TABLE visit_summaries (
  visit_date date,
  hll varbinary
);

INSERT INTO visit_summaries
SELECT visit_date, cast(approx_set(user_id) AS varbinary)
FROM user_visits
GROUP BY visit_date;

SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users
FROM visit_summaries
WHERE visit_date >= current_date - interval '7' day;
```

## Functions

::::{function} approx_set(x) -> HyperLogLog
Returns the `HyperLogLog` sketch of the input data set of `x`, using a fixed
default precision. This data sketch underlies {func}`approx_distinct` and can
be stored and used later by calling `cardinality()`.

:::{note}
The fixed default precision uses 4096 buckets, which corresponds to a standard
error of approximately 1.6% (`1.04 / sqrt(4096)`). To produce a sketch with this
same precision using the two-argument form, and thus one that can be merged with
sketches from `approx_set(x)`, pass a `maxStandardError` of `0.01625`.
:::
::::

::::{function} approx_set(x, maxStandardError) -> HyperLogLog
:noindex: true

Returns the `HyperLogLog` sketch of the input data set of `x`, with the
precision of the sketch controlled by `maxStandardError`. This is the desired
maximum standard error as a `double` between `0.0040625` and `0.26`, matching
{func}`approx_distinct`. A smaller value produces a more accurate sketch at the
cost of more memory during aggregation and a larger serialized size. Values
outside the permitted range fail the query.

:::{note}
Sketches can only be merged with, and produce a correct `cardinality()` union
for, other sketches of the **same** precision:

* Sketches produced with different `maxStandardError` values cannot be merged
  with each other. Every sketch that you intend to combine with {func}`merge`
  must be produced with the same `maxStandardError`.
* The single-argument `approx_set(x)` uses a fixed precision that generally
  differs from any explicit `maxStandardError`, so sketches from the two forms
  are generally not mergeable with each other.

Attempting to {func}`merge` sketches of different precision fails the query.
Reading a single stored sketch with `cardinality()` is unaffected.
:::
::::

:::{function} cardinality(hll) -> bigint
:noindex: true

This will perform {func}`approx_distinct` on the data summarized by the
`hll` HyperLogLog data sketch.
:::

:::{function} empty_approx_set() -> HyperLogLog
Returns an empty `HyperLogLog`.
:::

::::{function} merge(HyperLogLog) -> HyperLogLog
Returns the `HyperLogLog` of the aggregate union of the individual `hll`
HyperLogLog structures.

:::{note}
All sketches passed to a single `merge` must share the same precision (the same
number of buckets). This precision is determined by how each sketch was created:
`approx_set(x)` uses a fixed default precision, while `approx_set(x,
maxStandardError)` uses a precision derived from `maxStandardError`. Merging
sketches of different precision fails the query. Ensure every sketch you intend
to combine is produced the same way, with the same `maxStandardError`.
:::
::::
