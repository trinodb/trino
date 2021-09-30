===============
EXPLAIN ANALYZE
===============

Synopsis
--------

.. code-block:: text

    EXPLAIN ANALYZE [VERBOSE] statement

Description
-----------

Execute the statement and show the distributed execution plan of the statement
along with the cost of each operation.

The ``VERBOSE`` option will give more detailed information and low-level statistics;
understanding these may require knowledge of Trino internals and implementation details.

.. note::

    The stats may not be entirely accurate, especially for queries that complete quickly.

Examples
--------

In the example below, you can see the CPU time spent in each stage, as well as the relative
cost of each plan node in the stage. Note that the relative cost of the plan nodes is based on
wall time, which may or may not be correlated to CPU time. For each plan node you can see
some additional statistics (e.g: average input per node instance, average number of hash collisions for
relevant plan nodes). Such statistics are useful when one wants to detect data anomalies for a query
(skewness, abnormal hash collisions).

.. code-block:: sql

    EXPLAIN ANALYZE SELECT count(*), clerk FROM orders
    WHERE orderdate > date '1995-01-01' GROUP BY clerk;

.. code-block:: text

                                                                                                         Query Plan
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     Fragment 1 [HASH]
         CPU: 69.42ms, Scheduled: 168.23ms, Input: 3475 rows (128.96kB); per task: avg.: 3475.00 std.dev.: 0.00, Output: 1000 rows (28.32kB)
         Output layout: [clerk, count]
         Output partitioning: SINGLE []
         Stage Execution Strategy: UNGROUPED_EXECUTION
         Project[]
         │   Layout: [clerk:varchar(15), count:bigint]
         │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
         │   CPU: 17.00ms (0.41%), Scheduled: 65.00ms (0.42%), Output: 1000 rows (28.32kB)
         │   Input avg.: 62.50 rows, Input std.dev.: 14.77%
         └─ Aggregate(FINAL)[clerk][$hashvalue]
            │   Layout: [clerk:varchar(15), $hashvalue:bigint, count:bigint]
            │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
            │   CPU: 29.00ms (0.69%), Scheduled: 60.00ms (0.39%), Output: 1000 rows (37.11kB)
            │   Input avg.: 217.19 rows, Input std.dev.: 14.28%
            │   Collisions avg.: 0.00 (0.00% est.), Collisions std.dev.: ?%
            │   count := count("count_0")
            └─ LocalExchange[HASH][$hashvalue] ("clerk")
               │   Layout: [clerk:varchar(15), count_0:bigint, $hashvalue:bigint]
               │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
               │   CPU: 15.00ms (0.36%), Scheduled: 25.00ms (0.16%), Output: 3475 rows (128.96kB)
               │   Input avg.: 217.19 rows, Input std.dev.: 223.43%
               └─ RemoteSource[2]
                      Layout: [clerk:varchar(15), count_0:bigint, $hashvalue_1:bigint]
                      CPU: 1.00ms (0.02%), Scheduled: 2.00ms (0.01%), Output: 3475 rows (128.96kB)
                      Input avg.: 217.19 rows, Input std.dev.: 223.43%

     Fragment 2 [tpch:orders:15000]
         CPU: 4.14s, Scheduled: 15.40s, Input: 15000 rows (0B); per task: avg.: 15000.00 std.dev.: 0.00, Output: 3475 rows (128.96kB)
         Output layout: [clerk, count_0, $hashvalue_2]
         Output partitioning: HASH [clerk][$hashvalue_2]
         Stage Execution Strategy: UNGROUPED_EXECUTION
         Aggregate(PARTIAL)[clerk][$hashvalue_2]
         │   Layout: [clerk:varchar(15), $hashvalue_2:bigint, count_0:bigint]
         │   CPU: 84.00ms (2.00%), Scheduled: 294.00ms (1.89%), Output: 3475 rows (128.96kB)
         │   Input avg.: 2032.50 rows, Input std.dev.: 0.55%
         │   Collisions avg.: 42.76 (175.84% est.), Collisions std.dev.: 16.04%
         │   count_0 := count(*)
         └─ ScanFilterProject[table = tpch:orders:sf0.01, grouped = false, filterPredicate = ("orderdate" > DATE '1995-01-01')]
                Layout: [clerk:varchar(15), $hashvalue_2:bigint]
                Estimates: {rows: 15000 (219.73kB), cpu: 161.13k, memory: 0B, network: 0B}/{rows: 8164 (119.59kB), cpu: 322.27k, memory: 0B, network: 0B}/{rows: 8164 (119.59kB), cpu: 441.86k, memory: 0B, netw
                CPU: 4.05s (96.52%), Scheduled: 15.11s (97.13%), Output: 8130 rows (230.24kB)
                Input avg.: 3750.00 rows, Input std.dev.: 0.00%
                $hashvalue_2 := combine_hash(bigint '0', COALESCE("$operator$hash_code"("clerk"), 0))
                clerk := tpch:clerk
                orderdate := tpch:orderdate
                tpch:orderstatus
                    :: [[F], [O], [P]]
                Input: 15000 rows (0B), Filtered: 45.80%

When the ``VERBOSE`` option is used, some operators may report additional information.
For example, the window function operator will output the following::

    EXPLAIN ANALYZE VERBOSE SELECT count(clerk) OVER() FROM orders
    WHERE orderdate > date '1995-01-01';

.. code-block:: text

                                                                                                      Query Plan
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     Fragment 1 [SINGLE]
         CPU: 55.78ms, Scheduled: 112.49ms, Input: 8130 rows (158.79kB); per task: avg.: 8130.00 std.dev.: 0.00, Output: 8130 rows (71.46kB)
         Output layout: [count]
         Output partitioning: SINGLE []
         Stage Execution Strategy: UNGROUPED_EXECUTION
         Project[]
         │   Layout: [count:bigint]
         │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
         │   CPU: 5.00ms (0.45%), Scheduled: 22.00ms (0.45%), Output: 8130 rows (71.46kB)
         │   Input avg.: 508.13 rows, Input std.dev.: 387.30%
         └─ LocalExchange[ROUND_ROBIN] ()
            │   Layout: [clerk:varchar(15), count:bigint]
            │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
            │   CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Output: 8130 rows (230.24kB)
            │   Input avg.: 8130.00 rows, Input std.dev.: 0.00%
            └─ Window[]
               │   Layout: [clerk:varchar(15), count:bigint]
               │   CPU: 38.00ms (3.43%), Scheduled: 54.00ms (1.09%), Output: 8130 rows (230.24kB)
               │   Input avg.: 8130.00 rows, Input std.dev.: 0.00%
               │   Active Drivers: [ 1 / 1 ]
               │   Index size: std.dev.: 0.00 bytes, 0.00 rows
               │   Index count per driver: std.dev.: 0.00
               │   Rows per driver: std.dev.: 0.00
               │   Size of partition: std.dev.: 0.00
               │   count := count("clerk") RANGE UNBOUNDED_PRECEDING CURRENT_ROW
               └─ LocalExchange[SINGLE] ()
                  │   Layout: [clerk:varchar(15)]
                  │   Estimates: {rows: 8164 (47.84kB), cpu: 370.10k, memory: 0B, network: 47.84kB}
                  │   CPU: 1.00ms (0.09%), Scheduled: 1.00ms (0.02%), Output: 8130 rows (158.79kB)
                  │   Input avg.: 508.13 rows, Input std.dev.: 264.58%
                  └─ RemoteSource[2]
                         Layout: [clerk:varchar(15)]
                         CPU: 1.00ms (0.09%), Scheduled: 2.00ms (0.04%), Output: 8130 rows (158.79kB)
                         Input avg.: 508.13 rows, Input std.dev.: 264.58%

     Fragment 2 [tpch:orders:15000]
         CPU: 1.06s, Scheduled: 4.86s, Input: 15000 rows (0B); per task: avg.: 15000.00 std.dev.: 0.00, Output: 8130 rows (158.79kB)
         Output layout: [clerk]
         Output partitioning: SINGLE []
         Stage Execution Strategy: UNGROUPED_EXECUTION
         ScanFilterProject[table = tpch:orders:sf0.01, grouped = false, filterPredicate = ("orderdate" > DATE '1995-01-01')]
             Layout: [clerk:varchar(15)]
             Estimates: {rows: 15000 (87.89kB), cpu: 161.13k, memory: 0B, network: 0B}/{rows: 8164 (47.84kB), cpu: 322.27k, memory: 0B, network: 0B}/{rows: 8164 (47.84kB), cpu: 370.10k, memory: 0B, network: 0
             CPU: 1.06s (95.93%), Scheduled: 4.86s (98.40%), Output: 8130 rows (158.79kB)
             Input avg.: 3750.00 rows, Input std.dev.: 0.00%
             clerk := tpch:clerk
             orderdate := tpch:orderdate
             tpch:orderstatus
                 :: [[F], [O], [P]]
             Input: 15000 rows (0B), Filtered: 45.80%


See also
--------

:doc:`explain`
