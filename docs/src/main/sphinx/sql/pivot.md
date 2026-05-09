# PIVOT

## Synopsis

```text
PIVOT (
  aggregation [ [ AS ] aggregation_alias ] [, ...]
  FOR pivot_column [, (pivot_column [, ...]) ] IN ( pivot_value_group [, ...] )
  [ GROUP BY grouping_element [, ...] ]
  )
```

where `pivot_value_group` is one of

```text
expression [ [ AS ] value_alias ]
( expression, expression [, ...] ) [ [ AS ] value_alias ]
```

## Description

The `PIVOT` clause is an optional subclause of the `FROM` clause. It rotates
rows of an input relation into output columns by partitioning the rows on
one or more *pivot columns* and computing one or more *aggregations* for
each *pivot value*. The input to a pivot is a table, a view, or a subquery.
The output of a pivot is a relation, so it can itself appear in a `FROM`
clause, be aliased, or be the input to another `PIVOT`.

`PIVOT` is useful when each row in the input represents one observation
along a categorical dimension (such as a month, region, or status), and the
report should display one column per category. Common use cases include:

- summarizing measurements by time bucket,
- producing a column per status or category,
- comparing aggregated metrics side by side without writing repetitive
  `CASE` or `FILTER` expressions.

## Example

In the following example, `sales` records one row per region/month, and
`PIVOT` produces one column per month within each region:

```sql
SELECT *
FROM sales PIVOT (
    sum(amount) AS total
    FOR month IN (1 AS jan, 2 AS feb, 3 AS mar)
    GROUP BY region
    )
```

The output has columns `region`, `jan_total`, `feb_total`, `mar_total`.

In the following sections, all subclauses of the `PIVOT` clause are
explained.

## Aggregations

```sql
sum(amount) AS total
```

Each aggregation is an expression that contains one or more aggregate
function calls. The expression is evaluated once per pivot value, with each
aggregate scoped to the rows that match that pivot value.

The aggregation alias becomes part of the output column names (see
[](pivot-output)). Aliases are optional in the single-aggregation case but
required when a `PIVOT` declares more than one aggregation. Without
aliases, every output column for the multi-aggregation case would collide.

`PIVOT` accepts any expression Trino accepts as an aggregating select
item. For example, the following all work:

```sql
sum(amount)                                    -- single aggregate
avg(amount) * 100 AS pct                       -- expression over an aggregate
sum(amount) - sum(refund) AS net               -- multiple aggregates in one slot
```

When the slot expression contains multiple aggregate calls, the pivot
filter is applied to each aggregate individually, so `sum(amount) -
sum(refund)` filters both `sum`s to the rows for the current pivot value.

## Pivot column and IN list

```sql
FOR month IN (1 AS jan, 2 AS feb, 3 AS mar)
```

The `FOR` clause names the pivot column (or, for compound keys, a
parenthesised list of pivot columns) and the `IN` clause supplies the
values that become output columns. Each value is a constant expression and
is coerced to the pivot column's type using the standard implicit
coercion rules.

For multiple pivot columns, supply tuple values in matching order:

```sql
FOR (region, month) IN (('NA', 1) AS na_jan, ('EU', 1) AS eu_jan)
```

Each tuple must have the same arity as the pivot column list.

The value alias controls the output column name for that value. It is
strongly recommended in practice — without it, the column name is
derived from the SQL text of the value expression (so `1` and `'1'`
become distinct columns named `1` and `'1'`).

`NULL` is a permitted value, but it is treated using Trino's standard
`=` semantics: the predicate `pivot_column = NULL` is `UNKNOWN`, so the
corresponding output column always carries the empty-input aggregation
result (`NULL` for `sum`, `0` for `count`, and so on). To produce a
column for rows where the pivot column is `NULL`, supply that bucket
explicitly in the source relation rather than relying on a `NULL` IN
value.

## GROUP BY

```sql
GROUP BY region
```

The optional `GROUP BY` clause inside `PIVOT` controls which dimensions
are preserved as additional output columns. It accepts the same forms as
a top-level `GROUP BY`, including simple expressions, `()`, `GROUPING
SETS`, `CUBE`, and `ROLLUP`. Each grouping expression is projected as an
output column (with grouping-set semantics applied as usual).

When `GROUP BY` is omitted, no implicit grouping is performed: the result
is a single row whose only columns are the pivot output columns. To
preserve a dimension, name it explicitly in `GROUP BY`. This matches
Trino's general rule that aggregating queries collapse to a single row
unless `GROUP BY` says otherwise.

(pivot-output)=
## Output columns

The output column list is, in order:

1. The columns introduced by `GROUP BY` (in declaration order), if any.
2. One block per pivot value group (in declaration order).

Within each block, columns appear once per aggregation slot in the order
the aggregations were declared. The column name is determined by the
value alias and aggregation alias:

| Form | Output column name |
|---|---|
| Single aggregation, value alias provided | `valueAlias` |
| Single aggregation, no value alias | SQL text of the value |
| Multiple aggregations | `valueAlias_aggAlias` |
| Tuple value with tuple alias | `tupleAlias` |
| Tuple value without alias | components joined by `_` |

If two output columns would collide, an error is reported at analysis
time pointing at the colliding values.

## Pivot relation alias

A `PIVOT` clause may itself be aliased, with optional column aliases:

```sql
SELECT p.r, p.jan, p.feb
FROM sales PIVOT (
    sum(amount) FOR month IN (1 AS jan, 2 AS feb)
    GROUP BY region
    ) AS p (r, jan, feb)
```

The column-alias list, when present, must match the number of output
columns. This is the same form supported for subquery aliases.

## Restrictions

- `PIVOT` and {doc}`MATCH_RECOGNIZE</sql/match-recognize>` cannot apply to
  the same input relation. Wrap one in a subquery to combine them.
- The `IN` list does not support `ANY` or a subquery — values must be
  enumerated explicitly.
- `UNPIVOT` is not currently supported.

## See also

- {doc}`select`
- {doc}`match-recognize`
