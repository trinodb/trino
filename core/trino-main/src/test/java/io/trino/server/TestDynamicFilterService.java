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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.metadata.Metadata;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.server.DynamicFilterService.getOutboundDynamicFilters;
import static io.trino.server.DynamicFilterService.getSourceStageInnerLazyDynamicFilters;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.none;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.util.DynamicFiltersTestUtil.getSimplifiedDomainString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterService
{
    private final Metadata metadata = createTestMetadataManager();
    private final TypeOperators typeOperators = new TypeOperators();
    private static final Session session = TestingSession.testSessionBuilder().build();

    @Test
    public void testDynamicFilterSummaryCompletion()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId = new DynamicFilterId("df");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 3);
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        // assert initial dynamic filtering stats
        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getTotalDynamicFilters(), 1);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);
        assertEquals(stats.getLazyDynamicFilters(), 1);
        assertEquals(stats.getReplicatedDynamicFilters(), 0);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 1L)));
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1),
                ImmutableMap.of(filterId, singleValue(INTEGER, 2L)));
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 0);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 2),
                ImmutableMap.of(filterId, singleValue(INTEGER, 3L)));
        Optional<Domain> summary = dynamicFilterService.getSummary(queryId, filterId);
        assertTrue(summary.isPresent());
        assertEquals(summary.get(), multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 1);
        assertEquals(stats.getLazyDynamicFilters(), 1);
        assertEquals(stats.getReplicatedDynamicFilters(), 0);
        assertEquals(
                stats.getDynamicFilterDomainStats(),
                ImmutableList.of(new DynamicFilterDomainStats(
                        filterId,
                        getSimplifiedDomainString(1L, 3L, 3, INTEGER))));
    }

    @Test
    public void testDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        DynamicFilterId filterId3 = new DynamicFilterId("df3");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Symbol symbol2 = symbolAllocator.newSymbol("DF_SYMBOL2", INTEGER);
        Symbol symbol3 = symbolAllocator.newSymbol("DF_SYMBOL3", INTEGER);
        Expression df1 = symbol1.toSymbolReference();
        Expression df2 = symbol2.toSymbolReference();
        Expression df3 = symbol3.toSymbolReference();
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);
        StageId stageId2 = new StageId(queryId, 2);
        StageId stageId3 = new StageId(queryId, 3);

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1, filterId2, filterId3),
                ImmutableSet.of(filterId1, filterId2, filterId3),
                ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 2);
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId2, 2);
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId3, 2);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2),
                        new DynamicFilters.Descriptor(filterId3, df3)),
                ImmutableMap.of(
                        symbol1, new TestingColumnHandle("probeColumnA"),
                        symbol2, new TestingColumnHandle("probeColumnA"),
                        symbol3, new TestingColumnHandle("probeColumnB")),
                symbolAllocator.getTypes());

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

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));

        // tuple domain from two tasks are needed for dynamic filter to be narrowed down
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        assertFalse(blockedFuture.isDone());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 1),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 2L)));

        // dynamic filter (id1) has been collected as tuple domains from two tasks have been provided
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
        assertTrue(blockedFuture.isDone());
        assertFalse(blockedFuture.isCompletedExceptionally());

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 1);

        // there are still more dynamic filters to be collected
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        blockedFuture = dynamicFilter.isBlocked();
        assertFalse(blockedFuture.isDone());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId2, 0),
                ImmutableMap.of(filterId2, singleValue(INTEGER, 2L)));

        // tuple domain from two tasks (stage 2) are needed for dynamic filter to be narrowed down
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        assertFalse(blockedFuture.isDone());

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 1);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId2, 1),
                ImmutableMap.of(filterId2, singleValue(INTEGER, 3L)));

        // dynamic filter (id2) has been collected as tuple domains from two tasks have been provided
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));
        assertTrue(blockedFuture.isDone());
        assertFalse(blockedFuture.isCompletedExceptionally());

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
                        symbol1, new TestingColumnHandle("probeColumnA"),
                        symbol2, new TestingColumnHandle("probeColumnA")),
                symbolAllocator.getTypes());

        assertTrue(dynamicFilterColumnA.isComplete());
        assertFalse(dynamicFilterColumnA.isAwaitable());
        assertTrue(dynamicFilterColumnA.isBlocked().isDone());
        assertEquals(dynamicFilterColumnA.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId3, 0),
                ImmutableMap.of(filterId3, none(INTEGER)));

        // tuple domain from two tasks (stage 3) are needed for dynamic filter to be narrowed down
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));
        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isAwaitable());
        assertFalse(blockedFuture.isDone());

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 2);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId3, 1),
                ImmutableMap.of(filterId3, none(INTEGER)));

        // "none" dynamic filter (id3) has been collected for column B as tuple domains from two tasks have been provided
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.none());
        assertTrue(blockedFuture.isDone());
        assertFalse(blockedFuture.isCompletedExceptionally());

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertEquals(stats.getDynamicFiltersCompleted(), 3);
        assertEquals(stats.getLazyDynamicFilters(), 3);
        assertEquals(stats.getReplicatedDynamicFilters(), 0);
        assertEquals(ImmutableSet.copyOf(stats.getDynamicFilterDomainStats()), ImmutableSet.of(
                new DynamicFilterDomainStats(filterId1, getSimplifiedDomainString(1L, 2L, 2, INTEGER)),
                new DynamicFilterDomainStats(filterId2, getSimplifiedDomainString(2L, 3L, 2, INTEGER)),
                new DynamicFilterDomainStats(filterId3, Domain.none(INTEGER).toString(session.toConnectorSession()))));

        // all dynamic filters have been collected, no need for more requests
        assertTrue(dynamicFilter.isComplete());
        assertFalse(dynamicFilter.isAwaitable());
        assertTrue(dynamicFilter.isBlocked().isDone());
    }

    @Test
    public void testShortCircuitOnAllTupleDomain()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Expression df1 = symbol1.toSymbolReference();

        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 2);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(
                        symbol1, new TestingColumnHandle("probeColumnA")),
                symbolAllocator.getTypes());

        // dynamic filter is initially blocked
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertFalse(dynamicFilter.isComplete());
        assertFalse(dynamicFilter.isBlocked().isDone());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 1),
                ImmutableMap.of(filterId1, Domain.all(INTEGER)));

        // dynamic filter should be unblocked and completed
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertTrue(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isBlocked().isDone());
    }

    @Test
    public void testDynamicFilterCoercion()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Expression df1 = new Cast(symbol1.toSymbolReference(), toSqlType(BIGINT));

        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 1);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(symbol1, new TestingColumnHandle("probeColumnA")),
                symbolAllocator.getTypes());

        assertFalse(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0),
                ImmutableMap.of(filterId1, multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L))));
        assertTrue(dynamicFilter.isComplete());
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));
    }

    @Test
    public void testReplicatedDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Expression df1 = symbol1.toSymbolReference();
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(),
                ImmutableSet.of(filterId1));

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(
                        symbol1, new TestingColumnHandle("probeColumnA")),
                symbolAllocator.getTypes());
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

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));

        // tuple domain from single broadcast join task is sufficient
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 1L))));
        assertTrue(dynamicFilter.isComplete());
        assertFalse(dynamicFilter.isAwaitable());

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
                                Domain.singleValue(INTEGER, 1L).toString(session.toConnectorSession()))));
    }

    @Test
    public void testStageCannotScheduleMoreTasks()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Expression df1 = symbol1.toSymbolReference();
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(symbol1, new TestingColumnHandle("probeColumnA")),
                symbolAllocator.getTypes());
        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertFalse(dynamicFilter.isComplete());
        CompletableFuture<?> blockedFuture = dynamicFilter.isBlocked();
        assertFalse(blockedFuture.isDone());

        // adding task dynamic filters shouldn't complete dynamic filter
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));

        assertTrue(dynamicFilter.getCurrentPredicate().isAll());
        assertFalse(dynamicFilter.isComplete());
        assertFalse(blockedFuture.isDone());

        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 1);

        // dynamic filter should be completed when stage won't have more tasks
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 1L))));
        assertTrue(dynamicFilter.isComplete());
        assertTrue(blockedFuture.isDone());
        assertFalse(blockedFuture.isCompletedExceptionally());
    }

    @Test
    public void testDynamicFilterCancellation()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId = new DynamicFilterId("df");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Expression df1 = symbol1.toSymbolReference();
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 2);

        ColumnHandle column = new TestingColumnHandle("probeColumnA");
        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, df1)),
                ImmutableMap.of(symbol1, column),
                symbolAllocator.getTypes());
        assertFalse(dynamicFilter.isBlocked().isDone());
        assertFalse(dynamicFilter.isComplete());
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.all());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 1L)));
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.all());

        // DynamicFilter future cancellation should not affect DynamicFilterService
        CompletableFuture<?> isBlocked = dynamicFilter.isBlocked();
        assertFalse(isBlocked.isDone());
        assertFalse(isBlocked.cancel(false));
        assertFalse(dynamicFilter.isBlocked().isDone());
        assertFalse(dynamicFilter.isComplete());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1),
                ImmutableMap.of(filterId, singleValue(INTEGER, 2L)));
        assertTrue(isBlocked.isDone());
        assertTrue(dynamicFilter.isComplete());
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
    }

    @Test
    public void testIsAwaitable()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol = symbolAllocator.newSymbol("symbol", INTEGER);
        ColumnHandle handle = new TestingColumnHandle("probeColumnA");
        QueryId queryId = new QueryId("query");

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1, filterId2),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());

        DynamicFilter dynamicFilter1 = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId1, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, handle),
                symbolAllocator.getTypes());

        DynamicFilter dynamicFilter2 = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId2, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, handle),
                symbolAllocator.getTypes());

        assertTrue(dynamicFilter1.isAwaitable());
        // non lazy dynamic filters are marked as non-awaitable
        assertFalse(dynamicFilter2.isAwaitable());
    }

    @Test
    public void testMultipleColumnMapping()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Symbol symbol2 = symbolAllocator.newSymbol("DF_SYMBOL2", INTEGER);
        Expression df1 = symbol1.toSymbolReference();
        Expression df2 = symbol2.toSymbolReference();
        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 1);

        TestingColumnHandle column1 = new TestingColumnHandle("probeColumnA");
        TestingColumnHandle column2 = new TestingColumnHandle("probeColumnB");

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId1, df2)),
                ImmutableMap.of(
                        symbol1, column1,
                        symbol2, column2),
                symbolAllocator.getTypes());

        Domain domain = singleValue(INTEGER, 1L);
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0),
                ImmutableMap.of(filterId1, domain));

        assertEquals(
                dynamicFilter.getCurrentPredicate(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column1, domain,
                        column2, domain)));
    }

    @Test
    public void testDynamicFilterConsumer()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        Set<DynamicFilterId> dynamicFilters = ImmutableSet.of(filterId1, filterId2);
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, dynamicFilters, dynamicFilters, ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 2);

        Map<DynamicFilterId, Domain> consumerCollectedFilters = new HashMap<>();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                dynamicFilters,
                (domains) -> domains.forEach((filter, domain) -> assertNull(consumerCollectedFilters.put(filter, domain))));
        assertTrue(consumerCollectedFilters.isEmpty());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));
        assertTrue(consumerCollectedFilters.isEmpty());

        // complete only filterId1
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1),
                ImmutableMap.of(
                        filterId1, singleValue(INTEGER, 3L),
                        filterId2, singleValue(INTEGER, 2L)));
        assertEquals(consumerCollectedFilters, ImmutableMap.of(filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L))));

        // register another consumer only for filterId1 after completion of filterId1
        Map<DynamicFilterId, Domain> secondConsumerCollectedFilters = new HashMap<>();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                ImmutableSet.of(filterId1),
                (domains) -> domains.forEach((filter, domain) -> assertNull(secondConsumerCollectedFilters.put(filter, domain))));
        assertEquals(secondConsumerCollectedFilters, ImmutableMap.of(filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L))));

        // complete filterId2
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0),
                ImmutableMap.of(filterId2, singleValue(INTEGER, 4L)));
        assertEquals(
                consumerCollectedFilters,
                ImmutableMap.of(
                        filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L)),
                        filterId2, multipleValues(INTEGER, ImmutableList.of(2L, 4L))));
        assertEquals(
                secondConsumerCollectedFilters,
                ImmutableMap.of(filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L))));
    }

    @Test
    public void testDynamicFilterConsumerCallbackCount()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(metadata, typeOperators, newDirectExecutorService());
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        Set<DynamicFilterId> dynamicFilters = ImmutableSet.of(filterId1, filterId2);
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, dynamicFilters, dynamicFilters, ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 2);

        Map<DynamicFilterId, Domain> consumerCollectedFilters = new HashMap<>();
        AtomicInteger callbackCount = new AtomicInteger();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                dynamicFilters,
                (domains) -> {
                    callbackCount.getAndIncrement();
                    domains.forEach((filter, domain) -> assertNull(consumerCollectedFilters.put(filter, domain)));
                });
        assertTrue(consumerCollectedFilters.isEmpty());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0),
                ImmutableMap.of(
                        filterId1, singleValue(INTEGER, 1L),
                        filterId2, singleValue(INTEGER, 2L)));
        assertTrue(consumerCollectedFilters.isEmpty());

        // complete both filterId1 and filterId2
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1),
                ImmutableMap.of(
                        filterId1, singleValue(INTEGER, 3L),
                        filterId2, singleValue(INTEGER, 4L)));
        assertEquals(
                consumerCollectedFilters,
                ImmutableMap.of(
                        filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L)),
                        filterId2, multipleValues(INTEGER, ImmutableList.of(2L, 4L))));
        // both filters should be received in single callback
        assertEquals(callbackCount.get(), 1);

        // register another consumer after both filters have been collected
        Map<DynamicFilterId, Domain> secondConsumerCollectedFilters = new HashMap<>();
        AtomicInteger secondCallbackCount = new AtomicInteger();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                dynamicFilters,
                (domains) -> {
                    secondCallbackCount.getAndIncrement();
                    domains.forEach((filter, domain) -> assertNull(secondConsumerCollectedFilters.put(filter, domain)));
                });
        assertEquals(
                secondConsumerCollectedFilters,
                ImmutableMap.of(
                        filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L)),
                        filterId2, multipleValues(INTEGER, ImmutableList.of(2L, 4L))));
        // both filters should be received by second consumer in single callback
        assertEquals(secondCallbackCount.get(), 1);
        // first consumer should not receive callback again since it already got the completed filter
        assertEquals(callbackCount.get(), 1);
    }

    @Test
    public void testSourceStageInnerLazyDynamicFilters()
    {
        DynamicFilterId dynamicFilterId = new DynamicFilterId("filterId");
        assertEquals(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, SOURCE_DISTRIBUTION, REPLICATE)), ImmutableSet.of(dynamicFilterId));
        assertEquals(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, FIXED_HASH_DISTRIBUTION, REPLICATE)), ImmutableSet.of());
        assertEquals(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, SOURCE_DISTRIBUTION, REPARTITION)), ImmutableSet.of());
    }

    @Test
    public void testOutboundDynamicFilters()
    {
        DynamicFilterId filterId1 = new DynamicFilterId("filterId1");
        DynamicFilterId filterId2 = new DynamicFilterId("filterId2");
        assertEquals(getOutboundDynamicFilters(createPlan(filterId1, filterId1, FIXED_HASH_DISTRIBUTION, REPLICATE)), ImmutableSet.of());
        assertEquals(getOutboundDynamicFilters(createPlan(filterId1, filterId1, FIXED_HASH_DISTRIBUTION, REPARTITION)), ImmutableSet.of());
        assertEquals(getOutboundDynamicFilters(createPlan(filterId1, filterId2, FIXED_HASH_DISTRIBUTION, REPLICATE)), ImmutableSet.of(filterId1));
        assertEquals(getOutboundDynamicFilters(createPlan(filterId1, filterId2, FIXED_HASH_DISTRIBUTION, REPARTITION)), ImmutableSet.of(filterId1));
    }

    private static PlanFragment createPlan(DynamicFilterId dynamicFilterId, PartitioningHandle stagePartitioning, ExchangeNode.Type exchangeType)
    {
        return createPlan(dynamicFilterId, dynamicFilterId, stagePartitioning, exchangeType);
    }

    private static PlanFragment createPlan(
            DynamicFilterId consumedDynamicFilterId,
            DynamicFilterId producedDynamicFilterId,
            PartitioningHandle stagePartitioning,
            ExchangeNode.Type exchangeType)
    {
        Symbol symbol = new Symbol("column");
        Symbol buildSymbol = new Symbol("buildColumn");

        PlanNodeId tableScanNodeId = new PlanNodeId("plan_id");
        TableScanNode tableScan = TableScanNode.newInstance(
                tableScanNodeId,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingMetadata.TestingColumnHandle("column")),
                false,
                Optional.empty());
        FilterNode filterNode = new FilterNode(
                new PlanNodeId("filter_node_id"),
                tableScan,
                createDynamicFilterExpression(createTestMetadataManager(), consumedDynamicFilterId, VARCHAR, symbol.toSymbolReference()));

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
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(producedDynamicFilterId, buildSymbol),
                        Optional.empty()),
                ImmutableMap.of(symbol, VARCHAR),
                stagePartitioning,
                ImmutableList.of(tableScanNodeId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty());
    }
}
