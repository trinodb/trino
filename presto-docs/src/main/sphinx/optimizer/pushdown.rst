========
Pushdown
========

Presto can push down the processing of queries, or parts of queries, into the
connected data source. This means that a specific function, or other operation,
is passed through to the underlying database or storage system for processing.

The results of this pushdown can include the following benefits:

* improved overall query performance
* reduced network traffic between Presto and the data source
* reduced load on the remote data source

Support for pushdown is specific to each connector and the relevant underlying
database or storage system.

Analysis and Confirmation
-------------------------

Pushdown depends on a number of factors:

* generic support for pushdown for that function in Presto
* function or operation specific support for pushdown in the connector
* query that allows the detection of the function to push down
* function needs to exist in the underlying system so it can process the
  pushdown

The best way to analyze if pushdown for a specific query is performed is to
take a closer look at the :doc:`EXPLAIN plan </sql/explain>` of the query. If an
operation such as an aggregate function is successfully pushed down to the
connector, the explain plan does **not** show that operator. The explain plan
only shows the operations that are performed by Presto.

As an example, we loaded the TPCH data set into a PostgreSQL database and then
queried it using the PostgreSQL connector::

    SELECT regionkey, count(*)
    FROM nation
    GROUP BY regionkey;

You can get the explain plan by prepending the above query with ``EXPLAIN``::

    EXPLAIN
    SELECT regionkey, count(*)
    FROM nation
    GROUP BY regionkey;

The explain plan for this query does not show any ``Aggregate`` operator with
the ``count`` function, as this operation is now performed by the connector. You
can see the ``count(*)`` function as part of the PostgreSQL ``TableScan``
operator. This shows you that the pushdown was successful.

.. code-block:: none

    Fragment 0 [SINGLE]
        Output layout: [regionkey_0, _presto_generated_1]
        Output partitioning: SINGLE []
        Stage Execution Strategy: UNGROUPED_EXECUTION
        Output[regionkey, _col1]
        │   Layout: [regionkey_0:bigint, _presto_generated_1:bigint]
        │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}
        │   regionkey := regionkey_0
        │   _col1 := _presto_generated_1
        └─ RemoteSource[1]
                Layout: [regionkey_0:bigint, _presto_generated_1:bigint]

    Fragment 1 [SOURCE]
        Output layout: [regionkey_0, _presto_generated_1]
        Output partitioning: SINGLE []
        Stage Execution Strategy: UNGROUPED_EXECUTION
        TableScan[postgresql:tpch.nation tpch.nation columns=[regionkey:bigint:int8, count(*):_presto_generated_1:bigint:bigint] groupingSets=[[regionkey:bigint:int8]], gro
            Layout: [regionkey_0:bigint, _presto_generated_1:bigint]
            Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
            _presto_generated_1 := count(*):_presto_generated_1:bigint:bigint
            regionkey_0 := regionkey:bigint:int8

A number of factors can prevent a push down:

* adding a condition to the query
* using a different aggregate function without pushdown support in Presto
* using a function that has no native equivalent in the underlying data source
* using a connector without pushdown support for the specific function

As a result, the explain plan shows the ``Aggregate`` operation being performed
by Presto. This is a clear sign that now pushdown to the database is not
performed, and instead Presto performs the aggregate processing.

.. code-block:: none

 Fragment 0 [SINGLE]
     Output layout: [regionkey, count]
     Output partitioning: SINGLE []
     Stage Execution Strategy: UNGROUPED_EXECUTION
     Output[regionkey, _col1]
     │   Layout: [regionkey:bigint, count:bigint]
     │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
     │   _col1 := count
     └─ RemoteSource[1]
            Layout: [regionkey:bigint, count:bigint]

 Fragment 1 [HASH]
     Output layout: [regionkey, count]
     Output partitioning: SINGLE []
     Stage Execution Strategy: UNGROUPED_EXECUTION
     Aggregate(FINAL)[regionkey]
     │   Layout: [regionkey:bigint, count:bigint]
     │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
     │   count := count("count_0")
     └─ LocalExchange[HASH][$hashvalue] ("regionkey")
        │   Layout: [regionkey:bigint, count_0:bigint, $hashvalue:bigint]
        │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
        └─ RemoteSource[2]
               Layout: [regionkey:bigint, count_0:bigint, $hashvalue_1:bigint]

 Fragment 2 [SOURCE]
     Output layout: [regionkey, count_0, $hashvalue_2]
     Output partitioning: HASH [regionkey][$hashvalue_2]
     Stage Execution Strategy: UNGROUPED_EXECUTION
     Project[]
     │   Layout: [regionkey:bigint, count_0:bigint, $hashvalue_2:bigint]
     │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
     │   $hashvalue_2 := combine_hash(bigint '0', COALESCE("$operator$hash_code"("regionkey"), 0))
     └─ Aggregate(PARTIAL)[regionkey]
        │   Layout: [regionkey:bigint, count_0:bigint]
        │   count_0 := count(*)
        └─ TableScan[tpch:nation:sf0.01, grouped = false]
               Layout: [regionkey:bigint]
               Estimates: {rows: 25 (225B), cpu: 225, memory: 0B, network: 0B}
               regionkey := tpch:regionkey

Limitations
-----------

Pushdown does not support a number of more complex statements:

* complex grouping operations such as ``ROLLUP``, ``CUBE``, or ``GROUPING SETS``
* expressions inside the aggregation function call: ``sum(a * b)``
* coercions: ``sum(integer_column)``

