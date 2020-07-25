=========
Push down
=========

Presto can push down processing queries, or segments of queries, down to the
connected data source. This means that a specific function, or other operation,
is passed through to the underlying database for processing. An improved overall
query performance can be the result of this push down.

Support for push down is specific to each connector and the relevant underlying
database or storage system.

Every Presto aggregate function is eligible for push down, as long as it
operates on types supported by the given connector.

Analysis and Confirmation
-------------------------

Push down depends on a number of factors:

* generic support for push down in the connector
* function or operation specific support for push down in the connector
* query that allows the detection of the function to push down

The best way to analyze if push down for a specific query is performed, is to
take a closer look at the :doc:`EXPLAIN plan </sql/explain>` of the query. If an
operations such as an aggregate function successfully pushed down to the
connector, the explain plan does **not** show that operator. The explain plan
only shows the operations that are performed by Presto.

Here is an example query using the TPCH data, which was pushed into a PostgreSQL
database and it now queried with the PostgreSQL connector::

    USE postgresql.tpchtiny;
    SELECT regionkey, count(*)
    FROM nation
    GROUP BY 1;

You can get the explain plan with the following statement::

    EXPLAIN
    SELECT regionkey, count(*)
    FROM nation
    GROUP BY 1;

The explain plan for this query does not show any ``count`` operation as
``Aggregate`` in Presto. This is the result of a successful push down.

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
* using a different aggregate function without push down support
* using a connector without the specific function push down support

As a result the explain plan shows the ``Aggregat`` operation being performed by Presto, as a sign that now push down is performed:

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

Push down does not support a number of more complex statements:

* complex grouping sets such as ``GROUP BY GOUPING SETS``, ``ROLLUP`` or
    ``CUBE``
* expressions, like ``sum(a * b)``
* coercions, like ``sum(integer_column)``

