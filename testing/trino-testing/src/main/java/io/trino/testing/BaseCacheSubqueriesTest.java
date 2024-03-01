/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.cache.CacheDataOperator;
import io.trino.cache.CacheMetadata;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.cache.LoadCachedDataOperator;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.operator.ScanFilterAndProjectOperator;
import io.trino.operator.TableScanOperator;
import io.trino.operator.dynamicfiltering.DynamicPageFilterCache;
import io.trino.operator.dynamicfiltering.DynamicRowFilteringPageSourceProvider;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.split.AlternativeChooserPageSourceProvider;
import io.trino.split.SplitSource;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_COMMON_SUBQUERIES_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_PROJECTIONS_ENABLED;
import static io.trino.SystemSessionProperties.DYNAMIC_ROW_FILTERING_ENABLED;
import static io.trino.SystemSessionProperties.ENABLE_LARGE_DYNAMIC_FILTERS;
import static io.trino.cache.CommonSubqueriesExtractor.scanFilterProjectKey;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.cacheDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseCacheSubqueriesTest
        extends AbstractTestQueryFramework
{
    protected static final Set<TpchTable<?>> REQUIRED_TABLES = ImmutableSet.of(NATION, LINE_ITEM, ORDERS, CUSTOMER);
    protected static final Map<String, String> EXTRA_PROPERTIES = ImmutableMap.of("cache.enabled", "true");

    @BeforeEach
    public void flushCache()
    {
        getDistributedQueryRunner().getServers().forEach(server -> server.getCacheManagerRegistry().flushCache());
    }

    public static Object[][] isDynamicRowFilteringEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test
    public void testShowStats()
    {
        assertThat(query("SHOW STATS FOR nation"))
                .result()
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 25e0, 0e0, null)," +
                        "('name', 25e0, 0e0, null)," +
                        "('regionkey', 5e0, 0e0, null)," +
                        "('comment', 25e0, 0e0, null)," +
                        "(null, null, null, 25e0)");
    }

    @Test
    public void testJoinQuery()
    {
        @Language("SQL") String selectQuery = "select count(l.orderkey) from lineitem l, lineitem r where l.orderkey = r.orderkey";
        MaterializedResultWithPlan resultWithCache = executeWithPlan(withCacheEnabled(), selectQuery);
        MaterializedResultWithPlan resultWithoutCache = executeWithPlan(withCacheDisabled(), selectQuery);
        assertEqualsIgnoreOrder(resultWithCache.result(), resultWithoutCache.result());

        // make sure data was read from cache
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isPositive();

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();

        // make sure less data is read from source when caching is on
        assertThat(getScanOperatorInputPositions(resultWithCache.queryId()))
                .isLessThan(getScanOperatorInputPositions(resultWithoutCache.queryId()));
    }

    @Test
    public void testAggregationQuery()
    {
        @Language("SQL") String countQuery = """
                SELECT * FROM
                    (SELECT count(orderkey), orderkey FROM lineitem GROUP BY orderkey) a
                JOIN
                    (SELECT count(orderkey), orderkey FROM lineitem GROUP BY orderkey) b
                ON a.orderkey = b.orderkey""";
        @Language("SQL") String sumQuery = """
                SELECT * FROM
                    (SELECT sum(orderkey), orderkey FROM lineitem GROUP BY orderkey) a
                JOIN
                    (SELECT sum(orderkey), orderkey FROM lineitem GROUP BY orderkey) b
                ON a.orderkey = b.orderkey""";
        MaterializedResultWithPlan countWithCache = executeWithPlan(withCacheEnabled(), countQuery);
        MaterializedResultWithPlan countWithoutCache = executeWithPlan(withCacheDisabled(), countQuery);
        assertEqualsIgnoreOrder(countWithCache.result(), countWithoutCache.result());

        // make sure data was read from cache
        assertThat(getLoadCachedDataOperatorInputPositions(countWithCache.queryId())).isPositive();

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(countWithCache.queryId())).isPositive();

        // make sure less data is read from source when caching is on
        assertThat(getScanOperatorInputPositions(countWithCache.queryId()))
                .isLessThan(getScanOperatorInputPositions(countWithoutCache.queryId()));

        // subsequent count aggregation query should use cached data only
        countWithCache = executeWithPlan(withCacheEnabled(), countQuery);
        assertThat(getLoadCachedDataOperatorInputPositions(countWithCache.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(countWithCache.queryId())).isZero();

        // subsequent sum aggregation query should read from source as it doesn't match count plan signature
        MaterializedResultWithPlan sumWithCache = executeWithPlan(withCacheEnabled(), sumQuery);
        assertThat(getScanOperatorInputPositions(sumWithCache.queryId())).isPositive();
    }

    @Test
    public void testSubsequentQueryReadsFromCache()
    {
        @Language("SQL") String selectQuery = "select orderkey from lineitem union all (select orderkey from lineitem union all select orderkey from lineitem)";
        MaterializedResultWithPlan resultWithCache = executeWithPlan(withCacheEnabled(), selectQuery);

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();

        resultWithCache = executeWithPlan(withCacheEnabled(), "select orderkey from lineitem union all select orderkey from lineitem");
        // make sure data was read from cache as data should be cached across queries
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.queryId())).isZero();
    }

    @Test
    public void testSubsequentQueryReadsFromCacheWithPredicateOnDataColumn()
    {
        if (!supportsDataColumnPruning()) {
            abort("Data column pruning is not supported");
        }

        MaterializedResultWithPlan resultWithCache = executeWithPlan(
                withCacheEnabled(),
                "SELECT partkey FROM lineitem WHERE orderkey BETWEEN 0 AND 1000000000");

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();

        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                "SELECT partkey FROM lineitem WHERE orderkey BETWEEN 0 AND 1000000001");
        // make sure data was read from cache because both "orderkey BETWEEN 0 AND 1000000000"
        // and "orderkey BETWEEN 0 AND 1000000001" should evaluate to TRUE for lineitem splits
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.queryId())).isZero();

        // query with predicate that doesn't evaluate to TRUE for lineitem splits shouldn't read from cache
        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                "SELECT partkey FROM lineitem WHERE orderkey BETWEEN 0 AND 1000");
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isZero();
    }

    @Test
    public void testSubsequentQueryReadsFromCacheWithDynamicFilterOnDataColumn()
    {
        if (!supportsDataColumnPruning()) {
            abort("Data column pruning is not supported");
        }

        MaterializedResultWithPlan resultWithCache = executeWithPlan(
                withCacheEnabled(),
                """
                        SELECT partkey FROM lineitem l JOIN
                         (SELECT suppkey, orderkey FROM (VALUES (2, 17125), (3, 60000), (4, 60000)) t(suppkey, orderkey)) o
                        ON l.suppkey = o.suppkey AND l.orderkey <= o.orderkey
                        """);
        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanSplitsWithDynamicFiltersApplied(resultWithCache.queryId())).isPositive();

        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                """
                        SELECT partkey FROM lineitem l JOIN
                         (SELECT suppkey, orderkey FROM (VALUES (2, 17125), (3, 60000), (4, 60001)) t(suppkey, orderkey)) o
                        ON l.suppkey = o.suppkey AND l.orderkey <= o.orderkey
                        """);
        // make sure data was read from cache because dynamic filters for "l.orderkey < o.orderkey"
        // should evaluate to TRUE for both queries since the highest lineitem "orderkey" value is 60000
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.queryId())).isZero();

        // query with dynamic filter that doesn't evaluate to TRUE for lineitem splits shouldn't read from cache
        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                """
                        SELECT partkey FROM lineitem l JOIN
                         (SELECT suppkey, orderkey FROM (VALUES (2, 17125), (3, 59999), (4, 59999)) t(suppkey, orderkey)) o
                        ON l.suppkey = o.suppkey AND l.orderkey <= o.orderkey
                        """);
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isZero();
        assertThat(getScanSplitsWithDynamicFiltersApplied(resultWithCache.queryId())).isPositive();
    }

    @ParameterizedTest
    @MethodSource("isDynamicRowFilteringEnabled")
    public void testDynamicFilterCache(boolean isDynamicRowFilteringEnabled)
    {
        createPartitionedTableAsSelect("orders_part", ImmutableList.of("custkey"), "select orderkey, orderdate, orderpriority, mod(custkey, 10) as custkey from orders");
        @Language("SQL") String totalScanOrdersQuery = "select count(orderkey) from orders_part";
        @Language("SQL") String firstJoinQuery = """
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                """;
        @Language("SQL") String secondJoinQuery = """
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2, 4) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2, 3) t(custkey)) t on o.custkey = t.custkey
                """;
        @Language("SQL") String thirdJoinQuery = """
                select count(orderkey) from orders_part o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                """;

        Session cacheSubqueriesEnabled = withDynamicRowFiltering(withCacheEnabled(), isDynamicRowFilteringEnabled);
        Session cacheSubqueriesDisabled = withDynamicRowFiltering(withCacheDisabled(), isDynamicRowFilteringEnabled);
        MaterializedResultWithPlan totalScanOrdersExecution = executeWithPlan(cacheSubqueriesDisabled, totalScanOrdersQuery);
        MaterializedResultWithPlan firstJoinExecution = executeWithPlan(cacheSubqueriesEnabled, firstJoinQuery);
        MaterializedResultWithPlan anotherFirstJoinExecution = executeWithPlan(cacheSubqueriesEnabled, firstJoinQuery);
        MaterializedResultWithPlan secondJoinExecution = executeWithPlan(cacheSubqueriesEnabled, secondJoinQuery);
        MaterializedResultWithPlan thirdJoinExecution = executeWithPlan(cacheSubqueriesEnabled, thirdJoinQuery);

        // firstJoinQuery does not read whole probe side as some splits were pruned by dynamic filters
        assertThat(getScanOperatorInputPositions(firstJoinExecution.queryId())).isLessThan(getScanOperatorInputPositions(totalScanOrdersExecution.queryId()));
        assertThat(getCacheDataOperatorInputPositions(firstJoinExecution.queryId())).isPositive();
        // firstJoinQuery reads from table
        assertThat(getScanOperatorInputPositions(firstJoinExecution.queryId())).isPositive();
        // second run of firstJoinQuery reads only from cache
        assertThat(getScanOperatorInputPositions(anotherFirstJoinExecution.queryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(anotherFirstJoinExecution.queryId())).isPositive();

        // secondJoinQuery reads from table and cache because its predicate is wider that firstJoinQuery's predicate
        assertThat(getCacheDataOperatorInputPositions(secondJoinExecution.queryId())).isPositive();
        assertThat(getLoadCachedDataOperatorInputPositions(secondJoinExecution.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(secondJoinExecution.queryId())).isPositive();

        // thirdJoinQuery reads only from cache
        assertThat(getLoadCachedDataOperatorInputPositions(thirdJoinExecution.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(thirdJoinExecution.queryId())).isZero();

        assertUpdate("drop table orders_part");
    }

    @Test
    public void testPredicateOnPartitioningColumnThatWasNotFullyPushed()
    {
        createPartitionedTableAsSelect("orders_part", ImmutableList.of("orderkey"), "select orderdate, orderpriority, mod(orderkey, 50) as orderkey from orders");
        // mod predicate will be not pushed to connector
        @Language("SQL") String query =
                """
                        select * from (
                            select orderdate from orders_part where orderkey > 5 and mod(orderkey, 10) = 0 and orderpriority = '1-MEDIUM'
                            union all
                            select orderdate from orders_part where orderkey > 10 and mod(orderkey, 10) = 1 and orderpriority = '3-MEDIUM'
                        ) order by orderdate
                        """;
        MaterializedResultWithPlan cacheDisabledResult = executeWithPlan(withCacheDisabled(), query);
        executeWithPlan(withCacheEnabled(), query);
        MaterializedResultWithPlan cacheEnabledResult = executeWithPlan(withCacheEnabled(), query);

        assertThat(getLoadCachedDataOperatorInputPositions(cacheEnabledResult.queryId())).isPositive();
        assertThat(cacheDisabledResult.result()).isEqualTo(cacheEnabledResult.result());
        assertUpdate("drop table orders_part");
    }

    @Test
    public void testCacheWhenProjectionsWerePushedDown()
    {
        computeActual("create table orders_with_row (c row(name varchar, lastname varchar, age integer))");
        computeActual("insert into orders_with_row values (row (row ('any_name', 'any_lastname', 25)))");

        @Language("SQL") String query = "select c.name, c.age from orders_with_row union all select c.name, c.age from orders_with_row";
        @Language("SQL") String secondQuery = "select c.lastname, c.age from orders_with_row union all select c.lastname, c.age from orders_with_row";

        Session cacheEnabledProjectionDisabled = withProjectionPushdownEnabled(withCacheEnabled(), false);

        MaterializedResultWithPlan firstRun = executeWithPlan(withCacheEnabled(), query);
        assertThat(firstRun.result().getRowCount()).isEqualTo(2);
        assertThat(firstRun.result().getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
        assertThat(getCacheDataOperatorInputPositions(firstRun.queryId())).isPositive();

        // should use cache
        MaterializedResultWithPlan secondRun = executeWithPlan(withCacheEnabled(), query);
        assertThat(secondRun.result().getRowCount()).isEqualTo(2);
        assertThat(secondRun.result().getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
        assertThat(getLoadCachedDataOperatorInputPositions(secondRun.queryId())).isPositive();

        // shouldn't use cache because selected cacheColumnIds were different in the first case as projections were pushed down
        MaterializedResultWithPlan pushDownProjectionDisabledRun = executeWithPlan(cacheEnabledProjectionDisabled, query);
        assertThat(pushDownProjectionDisabledRun.result()).isEqualTo(firstRun.result());

        // shouldn't use cache because selected columns are different
        MaterializedResultWithPlan thirdRun = executeWithPlan(withCacheEnabled(), secondQuery);
        assertThat(getLoadCachedDataOperatorInputPositions(thirdRun.queryId())).isLessThanOrEqualTo(1);

        assertUpdate("drop table orders_with_row");
    }

    @Test
    public void testPartitionedQueryCache()
    {
        createPartitionedTableAsSelect("orders_part", ImmutableList.of("orderpriority"), "select orderkey, orderdate, orderpriority from orders");
        @Language("SQL") String selectTwoPartitions = """
                        select orderkey from orders_part where orderpriority IN ('3-MEDIUM', '1-URGENT')
                        union all
                        select orderkey from orders_part where orderpriority IN ('3-MEDIUM', '1-URGENT')
                """;
        @Language("SQL") String selectAllPartitions = """
                        select orderkey from orders_part
                        union all
                        select orderkey from orders_part
                """;
        @Language("SQL") String selectSinglePartition = """
                        select orderkey from orders_part where orderpriority = '3-MEDIUM'
                        union all
                        select orderkey from orders_part where orderpriority = '3-MEDIUM'
                """;

        MaterializedResultWithPlan twoPartitionsQueryFirst = executeWithPlan(withCacheEnabled(), selectTwoPartitions);
        Plan twoPartitionsQueryPlan = getDistributedQueryRunner().getQueryPlan(twoPartitionsQueryFirst.queryId());
        MaterializedResultWithPlan twoPartitionsQuerySecond = executeWithPlan(withCacheEnabled(), selectTwoPartitions);

        MaterializedResultWithPlan allPartitionsQuery = executeWithPlan(withCacheEnabled(), selectAllPartitions);
        Plan allPartitionsQueryPlan = getDistributedQueryRunner().getQueryPlan(allPartitionsQuery.queryId());

        String catalogId = withTransaction(session -> getDistributedQueryRunner().getCoordinator()
                .getPlannerContext().getMetadata()
                .getCatalogHandle(session, session.getCatalog().get())
                .orElseThrow()
                .getId());

        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(catalogId + ":" + getCacheTableId(getSession(), "orders_part"))),
                        Optional.empty(),
                        ImmutableList.of(getCacheColumnId(getSession(), "orders_part", "orderkey")),
                        ImmutableList.of(BIGINT)),
                TupleDomain.all());

        PlanMatchPattern chooseAlternativeNode = chooseAlternativeNode(
                tableScan("orders_part"),
                cacheDataPlanNode(tableScan("orders_part")),
                node(LoadCachedDataPlanNode.class)
                        .with(LoadCachedDataPlanNode.class, node -> node.getPlanSignature().equals(signature)));

        PlanMatchPattern originalPlanPattern = anyTree(chooseAlternativeNode, chooseAlternativeNode);

        // predicate for both original plans were pushed down to tableHandle what means that there is no
        // filter nodes. As a result, there is a same plan signatures for both (actually different) queries
        assertPlan(getSession(), twoPartitionsQueryPlan, originalPlanPattern);
        assertPlan(getSession(), allPartitionsQueryPlan, originalPlanPattern);

        // make sure that full scan reads data from table instead of basing on cache even though
        // plan signature is same
        assertThat(getScanOperatorInputPositions(twoPartitionsQueryFirst.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(twoPartitionsQuerySecond.queryId())).isZero();
        assertThat(getScanOperatorInputPositions(allPartitionsQuery.queryId())).isPositive();

        // notFilteringExecution should read from both cache (for partitions pre-loaded by filtering executions) and
        // from source table
        assertThat(getLoadCachedDataOperatorInputPositions(allPartitionsQuery.queryId())).isPositive();

        // single partition query should read from cache only because data for all partitions have been pre-loaded
        MaterializedResultWithPlan singlePartitionQuery = executeWithPlan(withCacheEnabled(), selectSinglePartition);
        assertThat(getScanOperatorInputPositions(singlePartitionQuery.queryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(singlePartitionQuery.queryId())).isPositive();

        // make sure that adding new partition doesn't invalidate existing cache entries
        computeActual("insert into orders_part values (-42, date '1991-01-01', 'foo')");
        singlePartitionQuery = executeWithPlan(withCacheEnabled(), selectSinglePartition);
        assertThat(getScanOperatorInputPositions(singlePartitionQuery.queryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(singlePartitionQuery.queryId())).isPositive();

        // validate results
        int twoPartitionsRowCount = twoPartitionsQueryFirst.result().getRowCount();
        assertThat(twoPartitionsRowCount).isEqualTo(twoPartitionsQuerySecond.result().getRowCount());
        assertThat(twoPartitionsRowCount).isLessThan(allPartitionsQuery.result().getRowCount());
        assertThat(singlePartitionQuery.result().getRowCount()).isLessThan(twoPartitionsRowCount);
        assertUpdate("drop table orders_part");
    }

    @ParameterizedTest
    @MethodSource("isDynamicRowFilteringEnabled")
    public void testGetUnenforcedPredicateAndPrunePredicate(boolean isDynamicRowFilteringEnabled)
    {
        String tableName = "get_unenforced_predicate_is_prune_and_prune_orders_part_" + isDynamicRowFilteringEnabled;
        createPartitionedTableAsSelect(tableName, ImmutableList.of("orderpriority"), "select orderkey, orderdate, '9876' as orderpriority from orders");
        DistributedQueryRunner runner = getDistributedQueryRunner();
        Session session = withDynamicRowFiltering(
                Session.builder(getSession())
                        .setQueryId(new QueryId("prune_predicate_" + isDynamicRowFilteringEnabled))
                        .build(),
                isDynamicRowFilteringEnabled);
        transaction(runner.getTransactionManager(), runner.getPlannerContext().getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    TestingTrinoServer coordinator = runner.getCoordinator();
                    TestingTrinoServer worker = runner.getServers().get(0);
                    checkState(!worker.isCoordinator());
                    String catalog = transactionSession.getCatalog().orElseThrow();
                    Metadata metadata = coordinator.getPlannerContext().getMetadata();
                    TableHandle handle = metadata.getTableHandle(
                            transactionSession,
                            new QualifiedObjectName(catalog, transactionSession.getSchema().orElseThrow(), tableName)).orElseThrow();
                    ConnectorTableHandle connectorTableHandle = handle.getConnectorHandle();

                    SplitSource splitSource = coordinator.getSplitManager().getSplits(transactionSession, Span.current(), handle, DynamicFilter.EMPTY, alwaysTrue());
                    ConnectorSplit split = getFutureValue(splitSource.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();

                    ColumnHandle partitionColumn = metadata.getColumnHandles(transactionSession, handle).get("orderpriority");
                    assertThat(partitionColumn).isNotNull();
                    ColumnHandle dataColumn = metadata.getColumnHandles(transactionSession, handle).get("orderkey");
                    assertThat(dataColumn).isNotNull();

                    ConnectorPageSourceProvider pageSourceProvider = getPageSourceProvider(worker.getConnector(coordinator.getCatalogHandle(catalog)));
                    DynamicRowFilteringPageSourceProvider dynamicRowFilteringPageSourceProvider = new DynamicRowFilteringPageSourceProvider(new DynamicPageFilterCache(new TypeOperators()));
                    VarcharType type = VarcharType.createVarcharType(4);

                    // getUnenforcedPredicate and prunePredicate should return none if predicate is exclusive on partition column
                    ConnectorSession connectorSession = transactionSession.toConnectorSession(metadata.getCatalogHandle(transactionSession, catalog).orElseThrow());
                    Domain nonPartitionDomain = Domain.multipleValues(type, Streams.concat(LongStream.range(0, 9_000), LongStream.of(9_999))
                            .boxed()
                            .map(value -> Slices.utf8Slice(value.toString()))
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);
                    assertThat(getUnenforcedPredicateIsPrune(
                            dynamicRowFilteringPageSourceProvider,
                            pageSourceProvider,
                            isDynamicRowFilteringEnabled,
                            session,
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);

                    // getUnenforcedPredicate and prunePredicate should prune prefilled column that matches given predicate fully
                    Domain partitionDomain = Domain.singleValue(type, Slices.utf8Slice("9876"));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);
                    assertThat(getUnenforcedPredicateIsPrune(
                            dynamicRowFilteringPageSourceProvider,
                            pageSourceProvider,
                            isDynamicRowFilteringEnabled,
                            session,
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);

                    // prunePredicate should not prune or simplify data column if there was no predicate on data column
                    Domain dataDomain = Domain.multipleValues(BIGINT, LongStream.range(0, 10_000)
                            .boxed()
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                            .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));

                    if (supportsDataColumnPruning()) {
                        SplitSource splitSourceWithDfOnDataColumn = coordinator.getSplitManager().getSplits(
                                transactionSession,
                                Span.current(),
                                handle,
                                getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                        dataColumn,
                                        Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1_000_000L)), false)))),
                                alwaysTrue());
                        ConnectorSplit splitWithDfOnDataColumn = getFutureValue(splitSourceWithDfOnDataColumn.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();
                        // getUnenforcedPredicate and prunePredicate should prune data column if there is dynamic filter on that column
                        Domain containingRange = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 60_000L)), false);
                        assertThat(pageSourceProvider.getUnenforcedPredicate(
                                connectorSession,
                                splitWithDfOnDataColumn,
                                connectorTableHandle,
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, containingRange))))
                                .isEqualTo(TupleDomain.all());
                        assertThat(pageSourceProvider.prunePredicate(
                                connectorSession,
                                splitWithDfOnDataColumn,
                                connectorTableHandle,
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, containingRange))))
                                .isEqualTo(TupleDomain.all());
                    }

                    if (isDynamicRowFilteringEnabled || getUnenforcedPredicateIsPrune()) {
                        // getUnenforcedPredicate should not prune or simplify data column
                        assertThat(dynamicRowFilteringPageSourceProvider.getUnenforcedPredicate(
                                pageSourceProvider,
                                session,
                                connectorSession,
                                split,
                                connectorTableHandle,
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));
                    }
                    else {
                        // getUnenforcedPredicate should not prune but simplify data column
                        assertThat(pageSourceProvider.getUnenforcedPredicate(
                                connectorSession,
                                split,
                                connectorTableHandle,
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, Domain.create(ValueSet.ofRanges(range(BIGINT, 0L, true, 9_999L, true)), false))));
                    }
                });
        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testEffectivePredicateReturnedPerSplit()
    {
        if (!effectivePredicateReturnedPerSplit()) {
            abort("Effective predicate is not returned per split");
        }

        DistributedQueryRunner runner = getDistributedQueryRunner();
        transaction(runner.getTransactionManager(), runner.getPlannerContext().getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(getSession(), transactionSession -> {
                    TestingTrinoServer coordinator = runner.getCoordinator();
                    TestingTrinoServer worker = runner.getServers().get(0);
                    checkState(!worker.isCoordinator());
                    String catalog = transactionSession.getCatalog().orElseThrow();
                    String schema = transactionSession.getSchema().orElseThrow();
                    Metadata metadata = coordinator.getPlannerContext().getMetadata();
                    TableHandle handle = metadata.getTableHandle(
                            transactionSession,
                            new QualifiedObjectName(catalog, schema, "lineitem")).orElseThrow();
                    ConnectorTableHandle connectorTableHandle = handle.getConnectorHandle();
                    ColumnHandle orderKeyColumn = metadata.getColumnHandles(transactionSession, handle).get("orderkey");

                    // get table handle with filter applied
                    TupleDomain<ColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                            orderKeyColumn, Domain.singleValue(BIGINT, 17125L)));
                    Optional<ConstraintApplicationResult<TableHandle>> filterResult = metadata.applyFilter(
                            transactionSession,
                            handle,
                            new Constraint(effectivePredicate));
                    assertThat(filterResult).isPresent();
                    TableHandle handleWithFilter = filterResult.get().getAlternatives().get(0).handle();
                    ConnectorTableHandle connectorTableHandleWithFilter = handleWithFilter.getConnectorHandle();

                    // make sure cache table ids are same for both table handles
                    CacheMetadata cacheMetadata = runner.getCacheMetadata();
                    assertThat(cacheMetadata.getCacheTableId(transactionSession, handle)).isEqualTo(cacheMetadata.getCacheTableId(transactionSession, handleWithFilter));

                    // make sure effective predicate is propagated as part of split id
                    SplitSource splitSource = coordinator.getSplitManager().getSplits(transactionSession, Span.current(), handle, DynamicFilter.EMPTY, alwaysTrue());
                    ConnectorSplit split = getFutureValue(splitSource.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();

                    SplitSource splitSourceWithFilter = coordinator.getSplitManager().getSplits(transactionSession, Span.current(), handleWithFilter, DynamicFilter.EMPTY, alwaysTrue());
                    ConnectorSplit splitWithFilter = getFutureValue(splitSourceWithFilter.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();

                    ConnectorPageSourceProvider pageSourceProvider = getPageSourceProvider(worker.getConnector(coordinator.getCatalogHandle(catalog)));
                    ConnectorSession connectorSession = transactionSession.toConnectorSession(metadata.getCatalogHandle(transactionSession, catalog).orElseThrow());

                    // split for original table handle doesn't propagate any effective predicate
                    assertThat(pageSourceProvider.getUnenforcedPredicate(connectorSession, split, connectorTableHandle, TupleDomain.all()))
                            .isEqualTo(TupleDomain.all());
                    // split for filtered table handle should propagate effective predicate
                    assertThat(pageSourceProvider.getUnenforcedPredicate(connectorSession, splitWithFilter, connectorTableHandleWithFilter, TupleDomain.all()))
                            .isEqualTo(effectivePredicate);

                    if (supportsDataColumnPruning()) {
                        // make sure prunePredicate removes predicates that evaluate to ALL for a split
                        assertThat(pageSourceProvider.prunePredicate(
                                connectorSession,
                                splitWithFilter,
                                connectorTableHandleWithFilter,
                                TupleDomain.withColumnDomains(ImmutableMap.of(
                                        orderKeyColumn, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 60_000L)), false)))))
                                .isEqualTo(TupleDomain.all());
                    }
                });
    }

    private ConnectorPageSourceProvider getPageSourceProvider(Connector workerConnector)
    {
        ConnectorPageSourceProvider pageSourceProvider = null;
        try {
            pageSourceProvider = workerConnector.getPageSourceProvider();
        }
        catch (UnsupportedOperationException ignored) {
        }

        try {
            pageSourceProvider = new AlternativeChooserPageSourceProvider(workerConnector.getAlternativeChooser());
        }
        catch (UnsupportedOperationException ignored) {
        }
        requireNonNull(pageSourceProvider, format("Connector '%s' returned a null page source provider", workerConnector));
        return pageSourceProvider;
    }

    private TupleDomain<ColumnHandle> getUnenforcedPredicateIsPrune(
            DynamicRowFilteringPageSourceProvider dynamicRowFilteringPageSourceProvider,
            ConnectorPageSourceProvider pageSourceProvider,
            boolean isDynamicRowFilteringEnabled,
            Session session,
            ConnectorSession connectorSession,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        if (isDynamicRowFilteringEnabled) {
            return dynamicRowFilteringPageSourceProvider.getUnenforcedPredicate(pageSourceProvider, session, connectorSession, split, table, predicate);
        }
        return pageSourceProvider.getUnenforcedPredicate(connectorSession, split, table, predicate);
    }

    protected CacheColumnId getCacheColumnId(Session session, String tableName, String columnName)
    {
        QueryRunner runner = getQueryRunner();
        QualifiedObjectName table = new QualifiedObjectName(session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), tableName);
        return transaction(runner.getTransactionManager(), runner.getPlannerContext().getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Metadata metadata = runner.getPlannerContext().getMetadata();
                    CacheMetadata cacheMetadata = runner.getCacheMetadata();
                    TableHandle tableHandle = metadata.getTableHandle(transactionSession, table).get();
                    return new CacheColumnId("[" + cacheMetadata.getCacheColumnId(transactionSession, tableHandle, metadata.getColumnHandles(transactionSession, tableHandle).get(columnName)).get() + "]");
                });
    }

    protected boolean effectivePredicateReturnedPerSplit()
    {
        return true;
    }

    protected boolean supportsDataColumnPruning()
    {
        return true;
    }

    protected boolean getUnenforcedPredicateIsPrune()
    {
        return false;
    }

    protected CacheTableId getCacheTableId(Session session, String tableName)
    {
        QueryRunner runner = getQueryRunner();
        QualifiedObjectName table = new QualifiedObjectName(session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), tableName);
        return transaction(runner.getTransactionManager(), runner.getPlannerContext().getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Metadata metadata = runner.getPlannerContext().getMetadata();
                    CacheMetadata cacheMetadata = runner.getCacheMetadata();
                    TableHandle tableHandle = metadata.getTableHandle(transactionSession, table).get();
                    return cacheMetadata.getCacheTableId(transactionSession, tableHandle).get();
                });
    }

    protected void assertPlan(Session session, Plan plan, PlanMatchPattern pattern)
    {
        QueryRunner runner = getQueryRunner();
        transaction(runner.getTransactionManager(), runner.getPlannerContext().getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    runner.getTransactionManager().getCatalogHandle(transactionSession.getTransactionId().get(), transactionSession.getCatalog().orElseThrow());
                    PlanAssert.assertPlan(transactionSession, getQueryRunner().getPlannerContext().getMetadata(), createTestingFunctionManager(), noopStatsCalculator(), plan, pattern);
                });
    }

    protected <T> T withTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return newTransaction().execute(getSession(), transactionSessionConsumer);
    }

    protected MaterializedResultWithPlan executeWithPlan(Session session, @Language("SQL") String sql)
    {
        return getDistributedQueryRunner().executeWithPlan(session, sql);
    }

    protected Long getScanSplitsWithDynamicFiltersApplied(QueryId queryId)
    {
        return getOperatorStats(queryId, TableScanOperator.class.getSimpleName(), ScanFilterAndProjectOperator.class.getSimpleName())
                .map(OperatorStats::getDynamicFilterSplitsProcessed)
                .mapToLong(Long::valueOf)
                .sum();
    }

    protected Long getScanOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, TableScanOperator.class.getSimpleName(), ScanFilterAndProjectOperator.class.getSimpleName());
    }

    protected Long getCacheDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, CacheDataOperator.class.getSimpleName());
    }

    protected Long getLoadCachedDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, LoadCachedDataOperator.class.getSimpleName());
    }

    protected Long getOperatorInputPositions(QueryId queryId, String... operatorType)
    {
        return getOperatorStats(queryId, operatorType)
                .map(OperatorStats::getInputPositions)
                .mapToLong(Long::valueOf)
                .sum();
    }

    protected Stream<OperatorStats> getOperatorStats(QueryId queryId, String... operatorType)
    {
        ImmutableSet<String> operatorTypes = ImmutableSet.copyOf(operatorType);
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> operatorTypes.contains(summary.getOperatorType()));
    }

    protected Session withCacheEnabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(ENABLE_LARGE_DYNAMIC_FILTERS, "false")
                .setSystemProperty(CACHE_COMMON_SUBQUERIES_ENABLED, "true")
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, "true")
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, "true")
                .build();
    }

    protected Session withCacheDisabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(ENABLE_LARGE_DYNAMIC_FILTERS, "false")
                .setSystemProperty(CACHE_COMMON_SUBQUERIES_ENABLED, "false")
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, "false")
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, "false")
                .build();
    }

    protected Session withDynamicRowFiltering(Session baseSession, boolean enabled)
    {
        return Session.builder(baseSession)
                .setSystemProperty(DYNAMIC_ROW_FILTERING_ENABLED, String.valueOf(enabled))
                .build();
    }

    abstract protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect);

    protected Session withProjectionPushdownEnabled(Session session, boolean projectionPushdownEnabled)
    {
        return session;
    }

    private static DynamicFilter getDynamicFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return tupleDomain.getDomains().map(Map::keySet)
                        .orElseGet(ImmutableSet::of);
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return tupleDomain;
            }

            @Override
            public OptionalLong getPreferredDynamicFilterTimeout()
            {
                return OptionalLong.of(0);
            }
        };
    }
}
