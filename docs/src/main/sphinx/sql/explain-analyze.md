# EXPLAIN ANALYZE

## Synopsis

```text
EXPLAIN ANALYZE [VERBOSE] statement
```

## Description

Execute the statement and show the distributed execution plan of the statement
along with the cost of each operation.

The `VERBOSE` option will give more detailed information and low-level statistics;
understanding these may require knowledge of Trino internals and implementation details.

:::{note}
The stats may not be entirely accurate, especially for queries that complete quickly.
:::

## Examples

In the example below, you can see the CPU time spent in each stage, as well as the relative
cost of each plan node in the stage. Note that the relative cost of the plan nodes is based on
wall time, which may or may not be correlated to CPU time. For each plan node you can see
some additional statistics (e.g: average input per node instance). Such statistics are useful
when one wants to detect data anomalies for a query (e.g: skewness).

```sql
EXPLAIN ANALYZE SELECT count(*), clerk FROM orders
WHERE orderdate > date '1995-01-01' GROUP BY clerk;
```

```text
                                          Query Plan
-----------------------------------------------------------------------------------------------
Trino version: version
Queued: 374.17us, Analysis: 190.96ms, Planning: 179.03ms, Execution: 3.06s
Fragment 1 [HASH]
    CPU: 22.58ms, Scheduled: 96.72ms, Blocked 46.21s (Input: 23.06s, Output: 0.00ns), Input: 1000 rows (37.11kB); per task: avg.: 1000.00 std.dev.: 0.00, Output: 1000 rows (28.32kB)
    Output layout: [clerk, count]
    Output partitioning: SINGLE []
    Project[]
    │   Layout: [clerk:varchar(15), count:bigint]
    │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
    │   CPU: 8.00ms (3.51%), Scheduled: 63.00ms (15.11%), Blocked: 0.00ns (0.00%), Output: 1000 rows (28.32kB)
    │   Input avg.: 15.63 rows, Input std.dev.: 24.36%
    └─ Aggregate[type = FINAL, keys = [clerk], hash = [$hashvalue]]
       │   Layout: [clerk:varchar(15), $hashvalue:bigint, count:bigint]
       │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
       │   CPU: 8.00ms (3.51%), Scheduled: 22.00ms (5.28%), Blocked: 0.00ns (0.00%), Output: 1000 rows (37.11kB)
       │   Input avg.: 15.63 rows, Input std.dev.: 24.36%
       │   count := count("count_0")
       └─ LocalExchange[partitioning = HASH, hashColumn = [$hashvalue], arguments = ["clerk"]]
          │   Layout: [clerk:varchar(15), count_0:bigint, $hashvalue:bigint]
          │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
          │   CPU: 2.00ms (0.88%), Scheduled: 4.00ms (0.96%), Blocked: 23.15s (50.10%), Output: 1000 rows (37.11kB)
          │   Input avg.: 15.63 rows, Input std.dev.: 793.73%
          └─ RemoteSource[sourceFragmentIds = [2]]
                 Layout: [clerk:varchar(15), count_0:bigint, $hashvalue_1:bigint]
                 CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Blocked: 23.06s (49.90%), Output: 1000 rows (37.11kB)
                 Input avg.: 15.63 rows, Input std.dev.: 793.73%

Fragment 2 [SOURCE]
    CPU: 210.60ms, Scheduled: 327.92ms, Blocked 0.00ns (Input: 0.00ns, Output: 0.00ns), Input: 1500000 rows (18.17MB); per task: avg.: 1500000.00 std.dev.: 0.00, Output: 1000 rows (37.11kB)
    Output layout: [clerk, count_0, $hashvalue_2]
    Output partitioning: HASH [clerk][$hashvalue_2]
    Aggregate[type = PARTIAL, keys = [clerk], hash = [$hashvalue_2]]
    │   Layout: [clerk:varchar(15), $hashvalue_2:bigint, count_0:bigint]
    │   CPU: 30.00ms (13.16%), Scheduled: 30.00ms (7.19%), Blocked: 0.00ns (0.00%), Output: 1000 rows (37.11kB)
    │   Input avg.: 818058.00 rows, Input std.dev.: 0.00%
    │   count_0 := count(*)
    └─ ScanFilterProject[table = hive:sf1:orders, filterPredicate = ("orderdate" > DATE '1995-01-01')]
           Layout: [clerk:varchar(15), $hashvalue_2:bigint]
           Estimates: {rows: 1500000 (41.48MB), cpu: 35.76M, memory: 0B, network: 0B}/{rows: 816424 (22.58MB), cpu: 35.76M, memory: 0B, network: 0B}/{rows: 816424 (22.58MB), cpu: 22.58M, memory: 0B, network: 0B}
           CPU: 180.00ms (78.95%), Scheduled: 298.00ms (71.46%), Blocked: 0.00ns (0.00%), Output: 818058 rows (12.98MB)
           Input avg.: 1500000.00 rows, Input std.dev.: 0.00%
           $hashvalue_2 := combine_hash(bigint '0', COALESCE("$operator$hash_code"("clerk"), 0))
           clerk := clerk:varchar(15):REGULAR
           orderdate := orderdate:date:REGULAR
           Input: 1500000 rows (18.17MB), Filtered: 45.46%, Physical Input: 4.51MB
```

When the `VERBOSE` option is used, some operators may report additional information.
For example, the window function operator will output the following:

```
EXPLAIN ANALYZE VERBOSE SELECT count(clerk) OVER() FROM orders
WHERE orderdate > date '1995-01-01';
```

```text
                                          Query Plan
-----------------------------------------------------------------------------------------------
  ...
         ─ Window[]
           │   Layout: [clerk:varchar(15), count:bigint]
           │   CPU: 157.00ms (53.40%), Scheduled: 158.00ms (37.71%), Blocked: 0.00ns (0.00%), Output: 818058 rows (22.62MB)
           │   metrics:
           │     'CPU time distribution (s)' = {count=1.00, p01=0.16, p05=0.16, p10=0.16, p25=0.16, p50=0.16, p75=0.16, p90=0.16, p95=0.16, p99=0.16, min=0.16, max=0.16}
           │     'Input rows distribution' = {count=1.00, p01=818058.00, p05=818058.00, p10=818058.00, p25=818058.00, p50=818058.00, p75=818058.00, p90=818058.00, p95=818058.00, p99=818058.00, min=818058.00, max=818058.00}
           │     'Scheduled time distribution (s)' = {count=1.00, p01=0.16, p05=0.16, p10=0.16, p25=0.16, p50=0.16, p75=0.16, p90=0.16, p95=0.16, p99=0.16, min=0.16, max=0.16}
           │   Input avg.: 818058.00 rows, Input std.dev.: 0.00%
           │   Active Drivers: [ 1 / 1 ]
           │   Index size: std.dev.: 0.00 bytes, 0.00 rows
           │   Index count per driver: std.dev.: 0.00
           │   Rows per driver: std.dev.: 0.00
           │   Size of partition: std.dev.: 0.00
           │   count := count("clerk") RANGE UNBOUNDED_PRECEDING CURRENT_ROW
 ...
```

## See also

{doc}`explain`
