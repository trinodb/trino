# DataSketches functions

DataSketches is a high-performance library of stochastic streaming
algorithms, commonly called sketches. Sketches are small, stateful programs
that process massive data as a stream and can provide approximate answers,
with mathematical guarantees, much faster than traditional exact methods.
The DataSketches functions allow querying the fast and memory-efficient
[Apache DataSketches](https://datasketches.apache.org/docs/Community/Research.html)
from Trino. Support for the
[Theta Sketch framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html)
is available through {func}`theta_sketch_union` and
{func}`theta_sketch_estimate`, typically used in `COUNT DISTINCT` queries.
DataSketches can be created with Hive or Pig using their respective sketch
APIs.

## Functions

:::{function} theta_sketch_union(sketches) -> sketch
Returns a single sketch which is a merged collection of sketches.
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

```sql
SELECT
  o_orderdate AS date,
  theta_sketch_estimate(theta_sketch_union(o_custkey_sketch)) AS unique_user_count,
  SUM(o_totalprice) AS user_spent
FROM tpch.sf100000.orders
WHERE o_orderdate >= dateadd(day, -90, current_date)
GROUP BY o_orderdate;
```
