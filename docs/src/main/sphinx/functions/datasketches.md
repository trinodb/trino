# DataSketches functions

[Apache DataSketches](https://datasketches.apache.org) is a high-performance
library of stochastic streaming algorithms (sketch algorithms) that produce
compact probabilistic summaries called sketches. A sketch is a small, stateful
data structure that processes massive data as a stream and can provide
approximate answers with mathematical guarantees much faster than traditional
exact methods. DataSketches functions allow querying these serialized sketches
from Trino. Support for the
[Theta Sketch framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)
is available through {func}`theta_sketch_union`, {func}`theta_sketch_intersection`,
and {func}`theta_sketch_cardinality`, typically used to replace expensive
`COUNT(DISTINCT ...)` and set-intersection aggregations when sketches are
precomputed and stored.

## Configuration

Because the DataSketches functions are provided by a connector, they are not
available by default. To enable them, you must configure a
[catalog properties file](catalog-properties) to register the functions with
the specified catalog name.

Create a catalog properties file `etc/catalog/datasketches.properties` that
references the `datasketches` connector:

```properties
connector.name=datasketches
```

The DataSketches functions are available with the `theta` schema name. For the
preceding example, the functions use the `datasketches.theta` catalog and
schema prefix.

To avoid needing to reference the functions with their fully qualified name,
configure the `sql.path` [SQL environment
property](/admin/properties-sql-environment) in the `config.properties` file to
include the catalog and schema prefix:

```properties
sql.path=datasketches.theta
```

Configure multiple catalogs to use the same functions with different
DataSketches configurations. In this case, the functions must be referenced
using their fully qualified name, rather than relying on the SQL path.

:::{note}
Trino does not create new sketches. Build Theta sketches upstream (for example,
in Spark, Hive, or Pig using the Apache DataSketches Theta APIs) and store the
serialized sketch bytes as a ``VARBINARY`` column. The Trino functions operate
on serialized Theta sketches only; other sketch families are not supported.
:::

## Functions

:::{function} theta_sketch_union(sketch [, nominal_entries, seed]) -> varbinary
Returns a serialized sketch as `varbinary`, which is a merged collection of
sketches. The optional `nominal_entries` and `seed` parameters let you specify
non-default sketch size and seed when merging sketches created with custom
settings.
:::

:::{function} theta_sketch_intersection(sketch) -> varbinary
Returns a serialized sketch as `varbinary` representing the intersection of all
input sketches. The result can be passed to {func}`theta_sketch_cardinality` to
obtain an estimate of the number of elements common to all inputs. All input
sketches must have been built with the default seed; use
{func}`theta_sketch_union` on the result if you need to compose intersection
with further aggregation.
:::

:::{function} theta_sketch_cardinality(sketch) -> double
Returns the estimated value of the sketch.
:::

::: {function} theta_sketch_cardinality(sketch, seed) -> double
:noindex: true

Returns the estimated value of the sketch using the supplied `seed`. Use this
when the sketch was created with a non-default seed.
:::

## Examples

The following query reads precomputed customer sketches from
`tpch.sf100000.orders`, unions them per order date, and produces an approximate
distinct customer count alongside exact spend. Using sketches avoids a heavy
`COUNT(DISTINCT ...)` over billions of rows while retaining predictable error
bounds.

```sql
SELECT
  o_orderdate AS date,
  theta_sketch_cardinality(theta_sketch_union(o_custkey_sketch)) AS unique_user_count,
  SUM(o_totalprice) AS user_spent
FROM tpch.sf100000.orders
GROUP BY o_orderdate;
```

For comparison, the exact equivalent requires the raw keys and a costly distinct
aggregation:

```sql
SELECT
  o_orderdate AS date,
  COUNT(DISTINCT o_custkey) AS unique_user_count,
  SUM(o_totalprice) AS user_spent
FROM tpch.sf100000.orders_raw_keys
GROUP BY o_orderdate;
```

The following query computes the approximate number of customers who placed
orders on both January 1 and January 2, by intersecting two precomputed daily
sketches:

```sql
SELECT theta_sketch_cardinality(theta_sketch_intersection(daily_sketch))
FROM daily_customer_sketches
WHERE order_date IN (DATE '2024-01-01', DATE '2024-01-02');
```
