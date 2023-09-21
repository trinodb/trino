# Cost-based optimizations

Trino supports several cost based optimizations, described below.

## Join enumeration

The order in which joins are executed in a query can have a significant impact
on the query's performance. The aspect of join ordering that has the largest
impact on performance is the size of the data being processed and transferred
over the network. If a join which produces a lot of data is performed early in
the query's execution, then subsequent stages need to process large amounts of
data for longer than necessary, increasing the time and resources needed for
processing the query.

With cost-based join enumeration, Trino uses {doc}`/optimizer/statistics`
provided by connectors to estimate the costs for different join orders and
automatically picks the join order with the lowest computed costs.

The join enumeration strategy is governed by the `join_reordering_strategy`
{ref}`session property <session-properties-definition>`, with the
`optimizer.join-reordering-strategy` configuration property providing the
default value.

The possible values are:

> - `AUTOMATIC` (default) - enable full automatic join enumeration
> - `ELIMINATE_CROSS_JOINS` - eliminate unnecessary cross joins
> - `NONE` - purely syntactic join order

If you are using `AUTOMATIC` join enumeration and statistics are not
available or a cost can not be computed for any other reason, the
`ELIMINATE_CROSS_JOINS` strategy is used instead.

## Join distribution selection

Trino uses a hash-based join algorithm. For each join operator, a hash table
must be created from one join input, referred to as the build side. The other
input, called the probe side, is then iterated on. For each row, the hash table
is queried to find matching rows.

There are two types of join distributions:

> - Partitioned: each node participating in the query builds a hash table from
>   only a fraction of the data
> - Broadcast: each node participating in the query builds a hash table from all
>   of the data. The data is replicated to each node.

Each type has advantages and disadvantages. Partitioned joins require
redistributing both tables using a hash of the join key. These joins can be much
slower than broadcast joins, but they allow much larger joins overall. Broadcast
joins are faster if the build side is much smaller than the probe side. However,
broadcast joins require that the tables on the build side of the join after
filtering fit in memory on each node, whereas distributed joins only need to fit
in distributed memory across all nodes.

With cost-based join distribution selection, Trino automatically chooses whether
to use a partitioned or broadcast join. With cost-based join enumeration, Trino
automatically chooses which sides are probe and build.

The join distribution strategy is governed by the `join_distribution_type`
session property, with the `join-distribution-type` configuration property
providing the default value.

The valid values are:

> - `AUTOMATIC` (default) - join distribution type is determined automatically
>   for each join
> - `BROADCAST` - broadcast join distribution is used for all joins
> - `PARTITIONED` - partitioned join distribution is used for all join

### Capping replicated table size

The join distribution type is automatically chosen when the join reordering
strategy is set to `AUTOMATIC` or when the join distribution type is set to
`AUTOMATIC`. In both cases, it is possible to cap the maximum size of the
replicated table with the `join-max-broadcast-table-size` configuration
property or with the `join_max_broadcast_table_size` session property. This
allows you to improve cluster concurrency and prevent bad plans when the
cost-based optimizer misestimates the size of the joined tables.

By default, the replicated table size is capped to 100MB.

## Syntactic join order

If not using cost-based optimization, Trino defaults to syntactic join ordering.
While there is no formal way to optimize queries for this case, it is possible
to take advantage of how Trino implements joins to make them more performant.

Trino uses in-memory hash joins. When processing a join statement, Trino loads
the right-most table of the join into memory as the build side, then streams the
next right-most table as the probe side to execute the join. If a query has
multiple joins, the result of this first join stays in memory as the build side,
and the third right-most table is then used as the probe side, and so on for
additional joins. In the case where join order is made more complex, such as
when using parentheses to specify specific parents for joins, Trino may execute
multiple lower-level joins at once, but each step of that process follows the
same logic, and the same applies when the results are ultimately joined
together.

Because of this behavior, it is optimal to syntactically order joins in your SQL
queries from the largest tables to the smallest, as this minimizes memory usage.

As an example, if you have a small, medium, and large table and are using left
joins:

```sql
SELECT
  *
FROM
  large_table l
  LEFT JOIN medium_table m ON l.user_id = m.user_id
  LEFT JOIN small_table s ON s.user_id = l.user_id
```

:::{warning}
This means of optimization is not a feature of Trino. It is an artifact of
how joins are implemented, and therefore this behavior may change without
notice.
:::

## Connector implementations

In order for the Trino optimizer to use the cost based strategies,
the connector implementation must provide {doc}`statistics`.
