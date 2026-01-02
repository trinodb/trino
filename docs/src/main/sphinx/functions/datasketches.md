# DataSketches functions

[Apache DataSketches](https://datasketches.apache.org) is a high-performance library of stochastic streaming
algorithms (sketch algorithms) that produce compact probabilistic summaries
called sketches. A sketch is a small, stateful data structure that processes
massive data as a stream and can provide approximate answers with mathematical
guarantees much faster than traditional exact methods. DataSketches functions
allow querying these serialized sketches from Trino. Support for the
[Theta Sketch framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)
is available through {func}`theta_sketch_union` and
{func}`theta_sketch_estimate`, typically used to replace expensive
`COUNT(DISTINCT ...)` aggregations when sketches are precomputed and stored.

## Configuration

Enable the functions by adding a catalog that uses the `datasketches` connector and exposes the `theta` schema.

Create a catalog properties file (for example, `etc/catalog/datasketches.properties`) that references the `datasketches` connector:

```properties
connector.name=datasketches
```

The functions are available with the `datasketches.theta` schema name. For the preceding example, the functions use the `datasketches.theta` catalog and schema prefix.

To avoid needing to reference the functions with their fully qualified name, configure the `sql.path` [SQL environment property](/admin/properties-sql-environment)
in the `config.properties` file to include the catalog and schema prefix:

```properties
sql.path=datasketches.theta
```

To use these functions, configure a catalog with the
``datasketches`` connector (for example, name the catalog ``datasketches``)
and either qualify function calls with ``datasketches.theta.`` or add
``datasketches.theta`` to the session ``path``.

:::{note}
Trino does not create new sketches. Build Theta sketches upstream (for example, in Spark, Hive, or
Pig using the Apache DataSketches Theta APIs) and store the serialized sketch bytes as a ``VARBINARY`` column.
The Trino functions operate on serialized Theta sketches only; other sketch families are not supported.
:::

## Functions

:::{function} theta_sketch_union(sketch [, nominal_entries, seed]) -> varbinary
Returns a serialized sketch as `varbinary`, which is a merged collection of sketches. The optional
`nominal_entries` and `seed` parameters let you specify non-default sketch size and seed when
merging sketches created with custom settings.
:::

:::{function} theta_sketch_estimate(sketch) -> double
Returns the estimated value of the sketch.
:::

::: {function} theta_sketch_estimate(sketch, seed) -> double
:noindex: true

Returns the estimated value of the sketch using the supplied `seed`. Use
this when the sketch was created with a non-default seed.
:::

## Examples

The following query reads precomputed customer sketches from `tpch.sf100000.orders`,
unions them per order date, and produces an approximate distinct
customer count alongside exact spend. Using sketches avoids a heavy `COUNT(DISTINCT ...)`
over billions of rows while retaining predictable error bounds.

```sql
SELECT
  o_orderdate AS date,
  theta_sketch_estimate(theta_sketch_union(o_custkey_sketch)) AS unique_user_count,
  SUM(o_totalprice) AS user_spent
FROM tpch.sf100000.orders
GROUP BY o_orderdate;
```

For comparison, the exact equivalent would need the raw keys and a costly distinct aggregation:

```sql
SELECT
  o_orderdate AS date,
  COUNT(DISTINCT o_custkey) AS unique_user_count,
  SUM(o_totalprice) AS user_spent
FROM tpch.sf100000.orders_raw_keys
GROUP BY o_orderdate;
```
