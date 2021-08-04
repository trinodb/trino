========
Pushdown
========

Trino can push down the processing of queries, or parts of queries, into the
connected data source. This means that a specific predicate, aggregation
function, or other operation, is passed through to the underlying database or
storage system for processing.

The results of this pushdown can include the following benefits:

* Improved overall query performance
* Reduced network traffic between Trino and the data source
* Reduced load on the remote data source

These benefits often result in significant cost reduction.

Support for pushdown is specific to each connector and the relevant underlying
database or storage system.

.. _predicate-pushdown:

Predicate pushdown
------------------

Predicate pushdown optimizes row-based filtering. It uses the inferred filter,
typically resulting from a condition in a ``WHERE`` clause to omit unnecessary
rows. The processing is pushed down to the data source by the connector and then
processed by the data source.

If predicate pushdown for a specific clause is succesful, the ``EXPLAIN`` plan
for the query does not include a ``ScanFilterProject`` operation for that
clause.

.. _projection-pushdown:

Projection pushdown
-------------------

Projection pushdown optimizes column-based filtering. It uses the columns
specified in the ``SELECT`` clause and other parts of the query to limit access
to these columns. The processing is pushed down to the data source by the
connector and then the data source only reads and returns the neccessary
columns.

If projection pushdown is succesful, the ``EXPLAIN`` plan for the query only
accesses the relevant columns in the ``Layout`` of the ``TableScan`` operation.

.. _aggregation-pushdown:

Aggregation pushdown
--------------------

Aggregation pushdown can take place provided the following conditions are satisfied:

* If aggregation pushdown is generally supported by the connector.
* If pushdown of the specific function or functions is supported by the connector.
* If the query structure allows pushdown to take place.

You can check if pushdown for a specific query is performed by looking at the
:doc:`EXPLAIN plan </sql/explain>` of the query. If an aggregate function is successfully
pushed down to the connector, the explain plan does **not** show that ``Aggregate`` operator.
The explain plan only shows the operations that are performed by Trino.

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

.. code-block:: text

    Fragment 0 [SINGLE]
        Output layout: [regionkey_0, _generated_1]
        Output partitioning: SINGLE []
        Stage Execution Strategy: UNGROUPED_EXECUTION
        Output[regionkey, _col1]
        │   Layout: [regionkey_0:bigint, _generated_1:bigint]
        │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}
        │   regionkey := regionkey_0
        │   _col1 := _generated_1
        └─ RemoteSource[1]
                Layout: [regionkey_0:bigint, _generated_1:bigint]

    Fragment 1 [SOURCE]
        Output layout: [regionkey_0, _generated_1]
        Output partitioning: SINGLE []
        Stage Execution Strategy: UNGROUPED_EXECUTION
        TableScan[postgresql:tpch.nation tpch.nation columns=[regionkey:bigint:int8, count(*):_generated_1:bigint:bigint] groupingSets=[[regionkey:bigint:int8]], gro
            Layout: [regionkey_0:bigint, _generated_1:bigint]
            Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
            _generated_1 := count(*):_generated_1:bigint:bigint
            regionkey_0 := regionkey:bigint:int8

A number of factors can prevent a push down:

* adding a condition to the query
* using a different aggregate function that cannot be pushed down into the connector
* using a connector without pushdown support for the specific function

As a result, the explain plan shows the ``Aggregate`` operation being performed
by Trino. This is a clear sign that now pushdown to the remote data source is not
performed, and instead Trino performs the aggregate processing.

.. code-block:: text

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
^^^^^^^^^^^

Aggregation pushdown does not support a number of more complex statements:

* complex grouping operations such as ``ROLLUP``, ``CUBE``, or ``GROUPING SETS``
* expressions inside the aggregation function call: ``sum(a * b)``
* coercions: ``sum(integer_column)``
* :ref:`aggregations with ordering <aggregate-function-ordering-during-aggregation>`
* :ref:`aggregations with filter <aggregate-function-filtering-during-aggregation>`

.. _join-pushdown:

Join pushdown
-------------

Join pushdown allows the connector to delegate the table join operation to the
underlying data source. This can result in performance gains, and allows Trino
to perform the remaining query processing on a smaller amount of data.

The specifics for the supported pushdown of table joins varies for each data
source, and therefore for each connector.

.. _limit-pushdown:

Limit pushdown
--------------

A :ref:`limit-clause` reduces the number of returned records for a statement.
Limit pushdown enables a connector to push processing of such queries of
unsorted record to the underlying data source.

A pushdown of this clause can improve the performance of the query and
significantly reduce the amount of data transferred from the data source to
Trino.

Queries include sections such as ``LIMIT N`` or ``FETCH FIRST N ROWS``.

Implementation and support is connector-specific since different data sources have varying capabilities.

.. _topn-pushdown:

Top-N pushdown
--------------

The combination of a :ref:`limit-clause` with an :ref:`order-by-clause` creates
a small set of records to return out of a large sorted dataset. It relies on the
order to determine which records need to be returned, and is therefore quite
different to optimize compared to a :ref:`limit-pushdown`.

The pushdown for such a query is called a Top-N pushdown, since the operation is
returning the top N rows. It enables a connector to push processing of such
queries to the underlying data source, and therefore significantly reduces the
amount of data transferred to and processed by Trino.

Queries include sections such as ``ORDER BY ... LIMIT N`` or ``ORDER BY ...
FETCH FIRST N ROWS``.

Implementation and support is connector-specific since different data sources
support different SQL syntax and processing.
