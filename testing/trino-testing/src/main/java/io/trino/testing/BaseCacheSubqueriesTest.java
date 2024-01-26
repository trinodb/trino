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
import io.trino.metadata.Split;
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
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.LongStream;

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
import static org.assertj.core.api.Assertions.assertThat;

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
    public void testSimplifyAndPrunePredicate(boolean isDynamicRowFilteringEnabled)
    {
        String tableName = "simplify_and_prune_orders_part_" + isDynamicRowFilteringEnabled;
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
                    Optional<TableHandle> handle = coordinator.getPlannerContext().getMetadata().getTableHandle(
                            transactionSession,
                            new QualifiedObjectName(catalog, transactionSession.getSchema().orElseThrow(), tableName));
                    assertThat(handle).isPresent();

                    SplitSource splitSource = coordinator.getSplitManager().getSplits(transactionSession, Span.current(), handle.get(), DynamicFilter.EMPTY, alwaysTrue());
                    Split split = getFutureValue(splitSource.getNextBatch(1000)).getSplits().get(0);

                    ColumnHandle partitionColumn = coordinator.getPlannerContext().getMetadata().getColumnHandles(transactionSession, handle.get()).get("orderpriority");
                    assertThat(partitionColumn).isNotNull();
                    ColumnHandle dataColumn = coordinator.getPlannerContext().getMetadata().getColumnHandles(transactionSession, handle.get()).get("orderkey");
                    assertThat(dataColumn).isNotNull();

                    ConnectorPageSourceProvider pageSourceProvider = getPageSourceProvider(worker.getConnector(coordinator.getCatalogHandle(catalog)));
                    DynamicRowFilteringPageSourceProvider dynamicRowFilteringPageSourceProvider = new DynamicRowFilteringPageSourceProvider(new DynamicPageFilterCache(new TypeOperators()));
                    VarcharType type = VarcharType.createVarcharType(4);

                    // simplifyPredicate and prunePredicate should return none if predicate is exclusive on partition column
                    ConnectorSession connectorSession = transactionSession.toConnectorSession(coordinator.getPlannerContext().getMetadata().getCatalogHandle(transactionSession, catalog).orElseThrow());
                    Domain nonPartitionDomain = Domain.multipleValues(type, Streams.concat(LongStream.range(0, 9_000), LongStream.of(9_999))
                            .boxed()
                            .map(value -> Slices.utf8Slice(value.toString()))
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);
                    assertThat(simplifyPredicate(
                            dynamicRowFilteringPageSourceProvider,
                            pageSourceProvider,
                            isDynamicRowFilteringEnabled,
                            session,
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);

                    // simplifyPredicate and prunePredicate should prune prefilled column that matches given predicate fully
                    Domain partitionDomain = Domain.singleValue(type, Slices.utf8Slice("9876"));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);
                    assertThat(simplifyPredicate(
                            dynamicRowFilteringPageSourceProvider,
                            pageSourceProvider,
                            isDynamicRowFilteringEnabled,
                            session,
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);

                    // prunePredicate should not prune or simplify data column
                    Domain dataDomain = Domain.multipleValues(BIGINT, LongStream.range(0, 10_000)
                            .boxed()
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                            .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));
                    if (isDynamicRowFilteringEnabled || simplifyIsPrune()) {
                        // simplifyPredicate should not prune or simplify data column
                        assertThat(dynamicRowFilteringPageSourceProvider.simplifyPredicate(
                                pageSourceProvider,
                                session,
                                connectorSession,
                                split.getConnectorSplit(),
                                handle.get().getConnectorHandle(),
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));
                    }
                    else {
                        // simplifyPredicate should not prune but simplify data column
                        assertThat(pageSourceProvider.getUnenforcedPredicate(
                                connectorSession,
                                split.getConnectorSplit(),
                                handle.get().getConnectorHandle(),
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, Domain.create(ValueSet.ofRanges(range(BIGINT, 0L, true, 9_999L, true)), false))));
                    }
                });
        assertUpdate("drop table " + tableName);
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

    private TupleDomain<ColumnHandle> simplifyPredicate(
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
            return dynamicRowFilteringPageSourceProvider.simplifyPredicate(pageSourceProvider, session, connectorSession, split, table, predicate);
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

    protected boolean simplifyIsPrune()
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
        ImmutableSet<String> operatorTypes = ImmutableSet.copyOf(operatorType);
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> operatorTypes.contains(summary.getOperatorType()))
                .map(OperatorStats::getInputPositions)
                .mapToLong(Long::valueOf)
                .sum();
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
}
