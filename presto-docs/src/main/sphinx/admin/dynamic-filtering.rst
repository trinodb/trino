=================
Dynamic Filtering
=================

Dynamic filtering optimizations significantly improve the performance of queries
with selective joins by avoiding reading of data that would be filtered by join condition.

Consider the following query which captures a common pattern of a fact table ``store_sales``
joined with a filtered dimension table ``date_dim``:

    SELECT count(*)
    FROM store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    WHERE d_following_holiday='Y' AND d_year = 2000;

Without dynamic filtering, Presto pushes predicates for the dimension table to the
table scan on ``date_dim``, and it scans all the data in the fact table since there
are no filters on ``store_sales`` in the query. The join operator ends up throwing away
most of the probe-side rows as the join criteria is highly selective.

When dynamic filtering is enabled, Presto collects candidate values for join condition
from the processed dimension table on the right side of join. In the case of broadcast joins,
the runtime predicates generated from this collection are pushed into the local table scan
on the left side of the join running on the same worker.

Additionally, these runtime predicates are communicated to the coordinator over the network
so that dynamic filtering can also be performed on the coordinator during enumeration of
table scan splits.

For example, in the case of the Hive connector, dynamic filters are used
to skip loading of partitions which don't match the join criteria.
This is known as **dynamic partition pruning**.

The results of dynamic filtering optimization can include the following benefits:

* improved overall query performance
* reduced network traffic between Presto and the data source
* reduced load on the remote data source

Support for push down of dynamic filters is specific to each connector,
and the relevant underlying database or storage system.

Analysis and confirmation
-------------------------

Dynamic filtering depends on a number of factors:

* Planner support for dynamic filtering for a given join operation in Presto.
  Currently inner and right joins with comparison join conditions and semi-joins with IN conditions are supported.
* Connector support for utilizing dynamic filters pushed into the table scan at runtime.
  For example, the Hive connector can push dynamic filters into ORC and Parquet readers
  to perform stripe or row-group pruning.
* Connector support for utilizing dynamic filters at the splits enumeration stage.
* Size of right (build) side of the join.

You can take a closer look at the :doc:`EXPLAIN plan </sql/explain>` of the query
to analyze if the planner is adding dynamic filters to a specific query's plan.
For example, the explain plan for the above query can be obtained by running
the following statement::

    EXPLAIN
    SELECT count(*)
    FROM store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    WHERE d_following_holiday='Y' AND d_year = 2000;

The explain plan for this query shows ``dynamicFilterAssignments`` in the
``InnerJoin`` node with dynamic filter ``df_370`` collected from build symbol ``d_date_sk``.
You can also see the ``dynamicFilter`` predicate as part of the Hive ``ScanFilterProject``
operator where ``df_370`` is associated with probe symbol ``ss_sold_date_sk``.
This shows you that the planner is successful in pushing dynamic filters
down to the connector in the query plan.

.. code-block:: none

    ...

    Fragment 1 [SOURCE]
        Output layout: [count_3]
        Output partitioning: SINGLE []
        Stage Execution Strategy: UNGROUPED_EXECUTION
        Aggregate(PARTIAL)
        │   Layout: [count_3:bigint]
        │   count_3 := count(*)
        └─ InnerJoin[(""ss_sold_date_sk"" = ""d_date_sk"")][$hashvalue, $hashvalue_4]
           │   Layout: []
           │   Estimates: {rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}
           │   Distribution: REPLICATED
           │   dynamicFilterAssignments = {d_date_sk -> df_370}
           ├─ ScanFilterProject[table = hive:default:store_sales, grouped = false, filterPredicate = true, dynamicFilter = {""ss_sold_date_sk"" = #df_370}]
           │      Layout: [ss_sold_date_sk:bigint, $hashvalue:bigint]
           │      Estimates: {rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}/{rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}/{rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}
           │      $hashvalue := combine_hash(bigint '0', COALESCE(""$operator$hash_code""(""ss_sold_date_sk""), 0))
           │      ss_sold_date_sk := ss_sold_date_sk:bigint:REGULAR
           └─ LocalExchange[HASH][$hashvalue_4] (""d_date_sk"")
              │   Layout: [d_date_sk:bigint, $hashvalue_4:bigint]
              │   Estimates: {rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}
              └─ RemoteSource[2]
                     Layout: [d_date_sk:bigint, $hashvalue_5:bigint]

    Fragment 2 [SOURCE]
        Output layout: [d_date_sk, $hashvalue_6]
        Output partitioning: BROADCAST []
        Stage Execution Strategy: UNGROUPED_EXECUTION
        ScanFilterProject[table = hive:default:date_dim, grouped = false, filterPredicate = ((""d_following_holiday"" = CAST('Y' AS char(1))) AND (""d_year"" = 2000))]
            Layout: [d_date_sk:bigint, $hashvalue_6:bigint]
            Estimates: {rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}/{rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}/{rows: 0 (0B), cpu: 0, memory: 0B, network: 0B}
            $hashvalue_6 := combine_hash(bigint '0', COALESCE(""$operator$hash_code""(""d_date_sk""), 0))
            d_following_holiday := d_following_holiday:char(1):REGULAR
            d_date_sk := d_date_sk:bigint:REGULAR
            d_year := d_year:int:REGULAR


During execution of a query with dynamic filters, Presto populates statistics
about dynamic filters in the QueryInfo JSON available through the
:doc:`/admin/web-interface`.
In the ``queryStats`` section, statistics about dynamic filters collected
by the coordinator can be found in the ``dynamicFiltersStats`` structure.

.. code-block:: none

    "dynamicFiltersStats" : {
          "dynamicFilterDomainStats" : [ {
            "dynamicFilterId" : "df_370",
            "simplifiedDomain" : "[ [[2451546, 2451905]] ]",
            "rangeCount" : 3,
            "discreteValuesCount" : 0,
            "collectionDuration" : "2.34s"
          } ],
          "lazyDynamicFilters" : 1,
          "replicatedDynamicFilters" : 1,
          "totalDynamicFilters" : 1,
          "dynamicFiltersCompleted" : 1
    }

Push down of dynamic filters into a table scan on the worker nodes can be
verified by looking at the operator statistics for that table scan.
``dynamicFilterSplitsProcessed`` records the number of splits
processed after a dynamic filter is pushed down to the table scan.

.. code-block:: none

    "operatorType" : "ScanFilterAndProjectOperator",
    "totalDrivers" : 1,
    "addInputCalls" : 762,
    "addInputWall" : "0.00ns",
    "addInputCpu" : "0.00ns",
    "physicalInputDataSize" : "0B",
    "physicalInputPositions" : 28800991,
    "inputPositions" : 28800991,
    "dynamicFilterSplitsProcessed" : 1,

Dynamic filter collection thresholds
------------------------------------

In order for dynamic filtering to work, the smaller dimension table
needs to be chosen as a join’s build side. The cost-based optimizer can automatically
do this using table statistics provided by connectors. Therefore, it is recommended
to keep :doc:`table statistics </optimizer/statistics>` up to date and rely on the
CBO to correctly choose the smaller table on the build side of join.

Collection of values of the join key columns from the build side for
dynamic filtering may incur additional CPU overhead during query execution.
Therefore, to limit the overhead of collecting dynamic filters
to the cases where the join operator is likely to be selective,
Presto defines thresholds on the size of dynamic filters collected from build side tasks.
Collection of dynamic filters for joins with large build sides can be enabled
using the ``enable-large-dynamic-filters`` configuration property or the
``enable_large_dynamic_filters`` session property.

When large dynamic filters are enabled, limits on the size of dynamic filters can
be configured for each join distribution type using the configuration properties
``dynamic-filtering.large-broadcast.max-distinct-values-per-driver``,
``dynamic-filtering.large-broadcast.max-size-per-driver`` and
``dynamic-filtering.large-broadcast.range-row-limit-per-driver`` and their
equivalents for partitioned join distribution type.

Similarly, limits for dynamic filters when ``enable-large-dynamic-filters``
is not enabled can be configured using configuration properties like
``dynamic-filtering.large-partitioned.max-distinct-values-per-driver``,
``dynamic-filtering.large-partitioned.max-size-per-driver`` and
``dynamic-filtering.large-partitioned.range-row-limit-per-driver`` and their
equivalent for broadcast join distribution type.

The properties based on ``max-distinct-values-per-driver`` and ``max-size-per-driver``
define thresholds for the size up to which dynamic filters are collected in a
distinct values data structure. When the build side exceeds these thresholds,
Presto switches to collecting min and max values per column to reduce overhead.
This min-max filter has much lower granularity than the distinct values filter.
However, it may still be beneficial in filtering some data from the probe side,
especially when a range of values is selected from the build side of the join.
The limits for min-max filters collection are defined by the properties
based on ``range-row-limit-per-driver``.

Dimension tables layout
-----------------------

Dynamic filtering works best for dimension tables where
table keys are correlated with columns.

For example, a date dimension key column should be correlated with a date column,
so the table keys monotonically increase with date values.
An address dimension key can be composed of other columns such as
``COUNTRY-STATE-ZIP-ADDRESS_ID`` with an example value of ``US-NY-10001-1234``.
This usage allows dynamic filtering to succeed even with a large number
of selected rows from the dimension table.

Limitations
-----------

* Dynamic filtering is currently implemented only for :doc:`/connector/hive` and :doc:`/connector/memory` connectors.
* Push down of dynamic filters into local table scan on worker nodes is limited to broadcast joins.
* Min-max dynamic filter collection is not supported for DOUBLE, REAL and unorderable data types.
* Dynamic filtering is supported when the join key contains a cast from the build key type to the
  probe key type. Dynamic filtering is also supported in limited scenarios when there is an implicit
  cast from the probe key type to the build key type. For example, dynamic filtering is supported when
  the build side key is of ``DOUBLE`` type and the probe side key is of ``REAL`` or ``INTEGER`` type.
