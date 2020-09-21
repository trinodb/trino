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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.DynamicFilterConfig;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageState;
import io.prestosql.execution.TaskId;
import io.prestosql.server.DynamicFilterService.StageDynamicFilters;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.StageState.RUNNING;
import static io.prestosql.execution.StageState.SCHEDULED;
import static io.prestosql.execution.StageState.SCHEDULING;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.server.DynamicFilterService.DynamicFilterDomainStats;
import static io.prestosql.server.DynamicFilterService.DynamicFiltersStats;
import static io.prestosql.server.DynamicFilterService.getSourceStageInnerLazyDynamicFilters;
import static io.prestosql.spi.predicate.Domain.multipleValues;
import static io.prestosql.spi.predicate.Domain.none;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.DynamicFilters.createDynamicFilterExpression;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.testing.TestingHandles.TEST_TABLE_HANDLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterService
{
    private static final Session session = TestingSession.testSessionBuilder().build();

    @Test
    public void testDynamicFilterSummaryCompletion()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new DynamicFilterConfig());
        DynamicFilterId filterId = new DynamicFilterId("df");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);
        List<TaskId> taskIds = ImmutableList.of(new TaskId(stageId, 0), new TaskId(stageId, 1), new TaskId(stageId, 2));

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(RUNNING);
        dynamicFiltersStageSupplier.addTasks(taskIds);
        dynamicFilterService.registerQuery(queryId, dynamicFiltersStageSupplier, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        // assert initial dynamic filtering stats
        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getTotalDynamicFilters(), 1);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);
        assertEquals(stats.getLazyDynamicFilters(), 1);
        assertEquals(stats.getReplicatedDynamicFilters(), 0);

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 0),
                singleValue(INTEGER, 1L));
        dynamicFilterService.collectDynamicFilters();
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 1),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 2);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 2),
                singleValue(INTEGER, 3L));
        dynamicFilterService.collectDynamicFilters();
        Optional<Domain> summary = dynamicFilterService.getSummary(queryId, filterId);
        assertTrue(summary.isPresent());
        assertEquals(summary.get(), multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 3);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 1);
        assertEquals(stats.getLazyDynamicFilters(), 1);
        assertEquals(stats.getReplicatedDynamicFilters(), 0);
        assertEquals(
                stats.getDynamicFilterDomainStats(),
                ImmutableList.of(new DynamicFilterDomainStats(
                        filterId, getExpectedDomainString(1L, 3L), 3, 0)));

        // all dynamic filters have been collected, no need for more requests
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 3);
    }

    @Test
    public void testDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new DynamicFilterConfig());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        DynamicFilterId filterId3 = new DynamicFilterId("df3");
        Expression df1 = expression("DF_SYMBOL1");
        Expression df2 = expression("DF_SYMBOL2");
        Expression df3 = expression("DF_SYMBOL3");
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);
        StageId stageId2 = new StageId(queryId, 2);
        StageId stageId3 = new StageId(queryId, 3);

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(RUNNING);

        dynamicFiltersStageSupplier.addTasks(ImmutableList.of(new TaskId(stageId1, 0), new TaskId(stageId1, 1)));
        dynamicFiltersStageSupplier.addTasks(ImmutableList.of(new TaskId(stageId2, 0), new TaskId(stageId2, 1)));
        dynamicFiltersStageSupplier.addTasks(ImmutableList.of(new TaskId(stageId3, 0), new TaskId(stageId3, 1)));

        dynamicFilterService.registerQuery(
                queryId,
                dynamicFiltersStageSupplier,
                ImmutableSet.of(filterId1, filterId2, filterId3),
                ImmutableSet.of(filterId1, filterId2, filterId3),
                ImmutableSet.of());

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2),
                        new DynamicFilters.Descriptor(filterId3, df3)),
                ImmutableMap.of(
                        Symbol.from(df1), new TestingColumnHandle("probeColumnA"),
                        Symbol.from(df2), new TestingColumnHandle("probeColumnA"),
                        Symbol.from(df3), new TestingColumnHandle("probeColumnB")));

        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());

        // assert initial dynamic filtering stats
        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getTotalDynamicFilters(), 3);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);
        assertEquals(stats.getLazyDynamicFilters(), 3);
        assertEquals(stats.getReplicatedDynamicFilters(), 0);

        // dynamic filter should be blocked waiting for tuple domain to be provided
        CompletableFuture<?> blockedFuture = dynamicFilter.isBlocked();
        assertFalse(blockedFuture.isDone());

        dynamicFiltersStageSupplier.storeSummary(
                filterId1,
                new TaskId(stageId1, 0),
                singleValue(INTEGER, 1L));
        dynamicFilterService.collectDynamicFilters();

        // tuple domain from two tasks are needed for dynamic filter to be narrowed down
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        assertFalse(blockedFuture.isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);

        dynamicFiltersStageSupplier.storeSummary(
                filterId1,
                new TaskId(stageId1, 1),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();

        // dynamic filter (id1) has been collected as tuple domains from two tasks have been provided
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
        assertTrue(blockedFuture.isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 2);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 1);

        // there are still more dynamic filters to be collected
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        blockedFuture = dynamicFilter.isBlocked();
        assertFalse(blockedFuture.isDone());

        dynamicFiltersStageSupplier.storeSummary(
                filterId2,
                new TaskId(stageId2, 0),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();

        // tuple domain from two tasks (stage 2) are needed for dynamic filter to be narrowed down
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        assertFalse(blockedFuture.isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 3);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 1);

        dynamicFiltersStageSupplier.storeSummary(
                filterId2,
                new TaskId(stageId2, 1),
                singleValue(INTEGER, 3L));
        dynamicFilterService.collectDynamicFilters();

        // dynamic filter (id2) has been collected as tuple domains from two tasks have been provided
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));
        assertTrue(blockedFuture.isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 4);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 2);

        // there are still more dynamic filters to be collected for columns A and B
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        blockedFuture = dynamicFilter.isBlocked();
        assertFalse(blockedFuture.isDone());

        // make sure dynamic filter on just column A is now completed
        DynamicFilter dynamicFilterColumnA = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2)),
                ImmutableMap.of(
                        Symbol.from(df1), new TestingColumnHandle("probeColumnA"),
                        Symbol.from(df2), new TestingColumnHandle("probeColumnA")));

        assertTrue(dynamicFilterColumnA.isComplete());
        assertFalse(dynamicFilterColumnA.isAwaitable());
        assertTrue(dynamicFilterColumnA.isBlocked().isDone());
        assertEquals(dynamicFilterColumnA.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));

        dynamicFiltersStageSupplier.storeSummary(
                filterId3,
                new TaskId(stageId3, 0),
                none(INTEGER));
        dynamicFilterService.collectDynamicFilters();

        // tuple domain from two tasks (stage 3) are needed for dynamic filter to be narrowed down
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        assertFalse(blockedFuture.isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 5);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 2);

        dynamicFiltersStageSupplier.storeSummary(
                filterId3,
                new TaskId(stageId3, 1),
                none(INTEGER));
        dynamicFilterService.collectDynamicFilters();

        // "none" dynamic filter (id3) has been collected for column B as tuple domains from two tasks have been provided
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.none());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 6);
        assertTrue(blockedFuture.isDone());

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 3);
        assertEquals(stats.getLazyDynamicFilters(), 3);
        assertEquals(stats.getReplicatedDynamicFilters(), 0);
        assertEquals(ImmutableSet.copyOf(stats.getDynamicFilterDomainStats()), ImmutableSet.of(
                new DynamicFilterDomainStats(
                        filterId1, getExpectedDomainString(1L, 2L), 2, 0),
                new DynamicFilterDomainStats(
                        filterId2, getExpectedDomainString(2L, 3L), 2, 0),
                new DynamicFilterDomainStats(
                        filterId3, Domain.none(INTEGER).toString(session.toConnectorSession()), 0, 0)));

        // all dynamic filters have been collected, no need for more requests
        dynamicFilterService.collectDynamicFilters();
        assertTrue(dynamicFilter.isComplete());
        assertFalse(dynamicFilter.isAwaitable());
        assertTrue(dynamicFilter.isBlocked().isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 6);
    }

    @Test
    public void testShortCircuitOnAllTupleDomain()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new DynamicFilterConfig());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        Expression df1 = expression("DF_SYMBOL1");

        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(RUNNING);
        dynamicFiltersStageSupplier.addTasks(ImmutableList.of(new TaskId(stageId1, 0), new TaskId(stageId1, 1)));

        dynamicFilterService.registerQuery(
                queryId,
                dynamicFiltersStageSupplier,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(
                        Symbol.from(df1), new TestingColumnHandle("probeColumnA")));

        // dynamic filter is initially blocked
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertFalse(dynamicFilter.isComplete());
        assertFalse(dynamicFilter.isBlocked().isDone());

        dynamicFiltersStageSupplier.storeSummary(
                filterId1,
                new TaskId(stageId1, 1),
                Domain.all(INTEGER));
        dynamicFilterService.collectDynamicFilters();

        // dynamic filter should be unblocked and completed
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertTrue(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isBlocked().isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);

        // all dynamic filters have been collected, no need for more requests
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);
    }

    @Test
    public void testReplicatedDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new DynamicFilterConfig());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        Expression df1 = expression("DF_SYMBOL1");
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(SCHEDULING);
        dynamicFiltersStageSupplier.addTasks(ImmutableList.of(new TaskId(stageId1, 0), new TaskId(stageId1, 1)));

        dynamicFilterService.registerQuery(
                queryId,
                dynamicFiltersStageSupplier,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(),
                ImmutableSet.of(filterId1));

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(
                        Symbol.from(df1), new TestingColumnHandle("probeColumnA")));
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());

        // assert initial dynamic filtering stats
        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getTotalDynamicFilters(), 1);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);
        assertEquals(stats.getReplicatedDynamicFilters(), 1);
        assertEquals(stats.getLazyDynamicFilters(), 0);

        // filterId1 wasn't marked as lazy
        assertFalse(dynamicFilter.isComplete());
        assertFalse(dynamicFilter.isAwaitable());
        assertTrue(dynamicFilter.isBlocked().isDone());

        dynamicFiltersStageSupplier.storeSummary(
                filterId1,
                new TaskId(stageId1, 0),
                singleValue(INTEGER, 1L));
        dynamicFilterService.collectDynamicFilters();

        // tuple domain from single broadcast join task is sufficient
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 1L))));
        assertTrue(dynamicFilter.isComplete());
        assertFalse(dynamicFilter.isAwaitable());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getTotalDynamicFilters(), 1);
        assertEquals(stats.getDynamicFiltersCompleted(), 1);
        assertEquals(stats.getReplicatedDynamicFilters(), 1);
        assertEquals(stats.getLazyDynamicFilters(), 0);
        assertEquals(
                stats.getDynamicFilterDomainStats(),
                ImmutableList.of(
                        new DynamicFilterDomainStats(
                                filterId1,
                                Domain.singleValue(INTEGER, 1L).toString(session.toConnectorSession()),
                                1,
                                0)));

        // all dynamic filters have been collected, no need for more requests
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);
    }

    @Test
    public void testDynamicFilterCancellation()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new DynamicFilterConfig());
        DynamicFilterId filterId = new DynamicFilterId("df");
        Expression df1 = expression("DF_SYMBOL1");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);
        List<TaskId> taskIds = ImmutableList.of(new TaskId(stageId, 0), new TaskId(stageId, 1));

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(RUNNING);
        dynamicFiltersStageSupplier.addTasks(taskIds);
        dynamicFilterService.registerQuery(queryId, dynamicFiltersStageSupplier, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        ColumnHandle column = new TestingColumnHandle("probeColumnA");
        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, df1)),
                ImmutableMap.of(
                        Symbol.from(df1), column));
        assertFalse(dynamicFilter.isBlocked().isDone());
        assertFalse(dynamicFilter.isComplete());
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.all());

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 0),
                singleValue(INTEGER, 1L));
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.all());

        // DynamicFilter future cancellation should not affect DynamicFilterService
        CompletableFuture<?> isBlocked = dynamicFilter.isBlocked();
        assertFalse(isBlocked.isDone());
        assertFalse(isBlocked.cancel(false));
        assertFalse(dynamicFilter.isBlocked().isDone());
        assertFalse(dynamicFilter.isComplete());

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 1),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();
        assertTrue(isBlocked.isDone());
        assertTrue(dynamicFilter.isComplete());
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
    }

    @Test
    public void testIsAwaitable()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new DynamicFilterConfig());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        Expression symbol = new Symbol("symbol").toSymbolReference();
        ColumnHandle handle = new TestingColumnHandle("probeColumnA");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 1);

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(SCHEDULING);
        dynamicFiltersStageSupplier.addTasks(ImmutableList.of(new TaskId(stageId, 0)));

        dynamicFilterService.registerQuery(
                queryId,
                dynamicFiltersStageSupplier,
                ImmutableSet.of(filterId1, filterId2),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());

        DynamicFilter dynamicFilter1 = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId1, symbol)),
                ImmutableMap.of(Symbol.from(symbol), handle));

        DynamicFilter dynamicFilter2 = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId2, symbol)),
                ImmutableMap.of(Symbol.from(symbol), handle));

        assertTrue(dynamicFilter1.isAwaitable());
        // non lazy dynamic filters are marked as non-awaitable
        assertFalse(dynamicFilter2.isAwaitable());
    }

    @Test
    public void testMultipleColumnMapping()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new DynamicFilterConfig());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        Expression df1 = expression("DF_SYMBOL1");
        Expression df2 = expression("DF_SYMBOL2");
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(SCHEDULED);
        dynamicFiltersStageSupplier.addTasks(ImmutableList.of(new TaskId(stageId1, 0)));

        dynamicFilterService.registerQuery(
                queryId,
                dynamicFiltersStageSupplier,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());

        TestingColumnHandle column1 = new TestingColumnHandle("probeColumnA");
        TestingColumnHandle column2 = new TestingColumnHandle("probeColumnB");

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId1, df2)),
                ImmutableMap.of(
                        Symbol.from(df1), column1,
                        Symbol.from(df2), column2));

        Domain domain = singleValue(INTEGER, 1L);
        dynamicFiltersStageSupplier.storeSummary(filterId1, new TaskId(stageId1, 0), domain);
        dynamicFilterService.collectDynamicFilters();

        assertEquals(
                dynamicFilter.getCurrentPredicate(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column1, domain,
                        column2, domain)));
    }

    @Test
    public void testSourceStageInnerLazyDynamicFilters()
    {
        DynamicFilterId dynamicFilterId = new DynamicFilterId("filterId");
        assertEquals(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, SOURCE_DISTRIBUTION, REPLICATE)), ImmutableSet.of(dynamicFilterId));
        assertEquals(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, FIXED_HASH_DISTRIBUTION, REPLICATE)), ImmutableSet.of());
        assertEquals(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, SOURCE_DISTRIBUTION, REPARTITION)), ImmutableSet.of());
    }

    private static PlanFragment createPlan(DynamicFilterId dynamicFilterId, PartitioningHandle stagePartitioning, ExchangeNode.Type exchangeType)
    {
        Symbol symbol = new Symbol("column");
        Symbol buildSymbol = new Symbol("buildColumn");

        PlanNodeId tableScanNodeId = new PlanNodeId("plan_id");
        TableScanNode tableScan = TableScanNode.newInstance(
                tableScanNodeId,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingMetadata.TestingColumnHandle("column")));
        FilterNode filterNode = new FilterNode(
                new PlanNodeId("filter_node_id"),
                tableScan,
                createDynamicFilterExpression(createTestMetadataManager(), dynamicFilterId, VARCHAR, symbol.toSymbolReference()));

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("remote_id"), new PlanFragmentId("plan_fragment_id"), ImmutableList.of(buildSymbol), Optional.empty(), exchangeType);
        return new PlanFragment(
                new PlanFragmentId("plan_id"),
                new JoinNode(new PlanNodeId("join_id"),
                        INNER,
                        filterNode,
                        remote,
                        ImmutableList.of(),
                        tableScan.getOutputSymbols(),
                        remote.getOutputSymbols(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(dynamicFilterId, buildSymbol),
                        Optional.empty()),
                ImmutableMap.of(symbol, VARCHAR),
                stagePartitioning,
                ImmutableList.of(tableScanNodeId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty());
    }

    private static String getExpectedDomainString(long low, long high)
    {
        return Domain.create(ValueSet.ofRanges(range(INTEGER, low, true, high, true)), false)
                .toString(session.toConnectorSession());
    }

    private static class TestDynamicFiltersStageSupplier
            implements Supplier<List<StageDynamicFilters>>
    {
        private final Map<StageId, Map<TaskId, Map<DynamicFilterId, Domain>>> stageDynamicFilters = new HashMap<>();
        private final StageState stageState;

        private int requestCount;

        TestDynamicFiltersStageSupplier(StageState stageState)
        {
            this.stageState = stageState;
        }

        void addTasks(List<TaskId> taskIds)
        {
            taskIds.forEach(taskId -> stageDynamicFilters
                    .computeIfAbsent(taskId.getStageId(), id -> new HashMap<>())
                    .put(taskId, new HashMap<>()));
        }

        void storeSummary(DynamicFilterId filterId, TaskId taskId, Domain domain)
        {
            StageId stageId = taskId.getStageId();
            stageDynamicFilters.get(stageId).get(taskId).put(filterId, domain);
        }

        int getRequestCount()
        {
            return requestCount;
        }

        @Override
        public List<StageDynamicFilters> get()
        {
            requestCount++;
            return ImmutableList.copyOf(stageDynamicFilters.values().stream()
                    .map(stage -> new StageDynamicFilters(
                            stageState,
                            stage.size(),
                            stage.values().stream()
                                    .collect(toImmutableList())))
                    .collect(toImmutableList()));
        }
    }
}
