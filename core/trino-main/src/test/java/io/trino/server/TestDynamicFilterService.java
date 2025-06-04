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
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
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
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.SystemSessionProperties.ENABLE_LARGE_DYNAMIC_FILTERS;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.server.DynamicFilterService.getOutboundDynamicFilters;
import static io.trino.server.DynamicFilterService.getSourceStageInnerLazyDynamicFilters;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.none;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.util.DynamicFiltersTestUtil.getSimplifiedDomainString;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicFilterService
{
    private static final Session session = TestingSession.testSessionBuilder()
            .setSystemProperty(ENABLE_LARGE_DYNAMIC_FILTERS, "false")
            .build();

    @Test
    public void testDynamicFilterSummaryCompletion()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        DynamicFilterId filterId = new DynamicFilterId("df");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 0, 3);
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        // assert initial dynamic filtering stats
        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getTotalDynamicFilters()).isEqualTo(1);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(0);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(1);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(0);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 1L)));
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(0);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 2L)));
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(0);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 2, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 3L)));
        Optional<Domain> summary = dynamicFilterService.getSummary(queryId, filterId);
        assertThat(summary).isPresent();
        assertThat(summary.get()).isEqualTo(multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(1);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(1);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(0);
        assertThat(stats.getDynamicFilterDomainStats()).isEqualTo(ImmutableList.of(new DynamicFilterDomainStats(
                filterId,
                getSimplifiedDomainString(1L, 3L, 3, INTEGER))));
    }

    @Test
    public void testDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
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
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 0, 2);
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId2, 0, 2);
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId3, 0, 2);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2),
                        new DynamicFilters.Descriptor(filterId3, df3)),
                ImmutableMap.of(
                        symbol1, new TestingColumnHandle("probeColumnA"),
                        symbol2, new TestingColumnHandle("probeColumnA"),
                        symbol3, new TestingColumnHandle("probeColumnB")));

        assertThat(dynamicFilter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(new TestingColumnHandle("probeColumnA"), new TestingColumnHandle("probeColumnB")));
        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isAwaitable()).isTrue();

        // assert initial dynamic filtering stats
        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getTotalDynamicFilters()).isEqualTo(3);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(0);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(3);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(0);

        // dynamic filter should be blocked waiting for tuple domain to be provided
        CompletableFuture<?> blockedFuture = dynamicFilter.isBlocked();
        assertThat(blockedFuture.isDone()).isFalse();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));

        // tuple domain from two tasks are needed for dynamic filter to be narrowed down
        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isAwaitable()).isTrue();
        assertThat(blockedFuture.isDone()).isFalse();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 1, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 2L)));

        // dynamic filter (id1) has been collected as tuple domains from two tasks have been provided
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
        assertThat(blockedFuture.isDone()).isTrue();
        assertThat(blockedFuture.isCompletedExceptionally()).isFalse();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(1);

        // there are still more dynamic filters to be collected
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isAwaitable()).isTrue();
        blockedFuture = dynamicFilter.isBlocked();
        assertThat(blockedFuture.isDone()).isFalse();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId2, 0, 0),
                ImmutableMap.of(filterId2, singleValue(INTEGER, 2L)));

        // tuple domain from two tasks (stage 2) are needed for dynamic filter to be narrowed down
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isAwaitable()).isTrue();
        assertThat(blockedFuture.isDone()).isFalse();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(1);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId2, 1, 0),
                ImmutableMap.of(filterId2, singleValue(INTEGER, 3L)));

        // dynamic filter (id2) has been collected as tuple domains from two tasks have been provided
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));
        assertThat(blockedFuture.isDone()).isTrue();
        assertThat(blockedFuture.isCompletedExceptionally()).isFalse();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(2);

        // there are still more dynamic filters to be collected for columns A and B
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isAwaitable()).isTrue();
        blockedFuture = dynamicFilter.isBlocked();
        assertThat(blockedFuture.isDone()).isFalse();

        // make sure dynamic filter on just column A is now completed
        DynamicFilter dynamicFilterColumnA = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId2, df2)),
                ImmutableMap.of(
                        symbol1, new TestingColumnHandle("probeColumnA"),
                        symbol2, new TestingColumnHandle("probeColumnA")));

        assertThat(dynamicFilterColumnA.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(new TestingColumnHandle("probeColumnA")));
        assertThat(dynamicFilterColumnA.isComplete()).isTrue();
        assertThat(dynamicFilterColumnA.isAwaitable()).isFalse();
        assertThat(dynamicFilterColumnA.isBlocked().isDone()).isTrue();
        assertThat(dynamicFilterColumnA.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId3, 0, 0),
                ImmutableMap.of(filterId3, none(INTEGER)));

        // tuple domain from two tasks (stage 3) are needed for dynamic filter to be narrowed down
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 2L))));
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isAwaitable()).isTrue();
        assertThat(blockedFuture.isDone()).isFalse();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(2);

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId3, 1, 0),
                ImmutableMap.of(filterId3, none(INTEGER)));

        // "none" dynamic filter (id3) has been collected for column B as tuple domains from two tasks have been provided
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.none());
        assertThat(blockedFuture.isDone()).isTrue();
        assertThat(blockedFuture.isCompletedExceptionally()).isFalse();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(3);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(3);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(0);
        assertThat(ImmutableSet.copyOf(stats.getDynamicFilterDomainStats())).isEqualTo(ImmutableSet.of(
                new DynamicFilterDomainStats(filterId1, getSimplifiedDomainString(1L, 2L, 2, INTEGER)),
                new DynamicFilterDomainStats(filterId2, getSimplifiedDomainString(2L, 3L, 2, INTEGER)),
                new DynamicFilterDomainStats(filterId3, none(INTEGER).toString(session.toConnectorSession()))));

        // all dynamic filters have been collected, no need for more requests
        assertThat(dynamicFilter.isComplete()).isTrue();
        assertThat(dynamicFilter.isAwaitable()).isFalse();
        assertThat(dynamicFilter.isBlocked().isDone()).isTrue();
    }

    @Test
    public void testShortCircuitOnAllTupleDomain()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
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
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 0, 2);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(
                        symbol1, new TestingColumnHandle("probeColumnA")));

        // dynamic filter is initially blocked
        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isBlocked().isDone()).isFalse();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 1, 0),
                ImmutableMap.of(filterId1, Domain.all(INTEGER)));

        // dynamic filter should be unblocked and completed
        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();
        assertThat(dynamicFilter.isComplete()).isTrue();
        assertThat(dynamicFilter.isBlocked().isDone()).isTrue();
    }

    @Test
    public void testDynamicFilterCoercion()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Expression df1 = new Cast(symbol1.toSymbolReference(), BIGINT);

        QueryId queryId = new QueryId("query");
        StageId stageId1 = new StageId(queryId, 1);

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(filterId1),
                ImmutableSet.of(filterId1),
                ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 0, 1);

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId1, df1)),
                ImmutableMap.of(symbol1, new TestingColumnHandle("probeColumnA")));

        assertThat(dynamicFilter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(new TestingColumnHandle("probeColumnA")));
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0, 0),
                ImmutableMap.of(filterId1, multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L))));
        assertThat(dynamicFilter.isComplete()).isTrue();
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));
    }

    @Test
    public void testReplicatedDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
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
                        symbol1, new TestingColumnHandle("probeColumnA")));

        assertThat(dynamicFilter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(new TestingColumnHandle("probeColumnA")));
        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();

        // assert initial dynamic filtering stats
        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getTotalDynamicFilters()).isEqualTo(1);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(0);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(1);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(0);

        // filterId1 wasn't marked as lazy
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.isAwaitable()).isFalse();
        assertThat(dynamicFilter.isBlocked().isDone()).isTrue();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));

        // tuple domain from single broadcast join task is sufficient
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 1L))));
        assertThat(dynamicFilter.isComplete()).isTrue();
        assertThat(dynamicFilter.isAwaitable()).isFalse();

        stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getTotalDynamicFilters()).isEqualTo(1);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(1);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(1);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(0);
        assertThat(stats.getDynamicFilterDomainStats()).isEqualTo(ImmutableList.of(
                new DynamicFilterDomainStats(
                        filterId1,
                        singleValue(INTEGER, 1L).toString(session.toConnectorSession()))));
    }

    @Test
    public void testStageCannotScheduleMoreTasks()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
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
                ImmutableMap.of(symbol1, new TestingColumnHandle("probeColumnA")));
        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();
        assertThat(dynamicFilter.isComplete()).isFalse();
        CompletableFuture<?> blockedFuture = dynamicFilter.isBlocked();
        assertThat(blockedFuture.isDone()).isFalse();

        // adding task dynamic filters shouldn't complete dynamic filter
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));

        assertThat(dynamicFilter.getCurrentPredicate().isAll()).isTrue();
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(blockedFuture.isDone()).isFalse();

        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 0, 1);

        // dynamic filter should be completed when stage won't have more tasks
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                new TestingColumnHandle("probeColumnA"),
                singleValue(INTEGER, 1L))));
        assertThat(dynamicFilter.isComplete()).isTrue();
        assertThat(blockedFuture.isDone()).isTrue();
        assertThat(blockedFuture.isCompletedExceptionally()).isFalse();
    }

    @Test
    public void testDynamicFilterCancellation()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        DynamicFilterId filterId = new DynamicFilterId("df");
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("DF_SYMBOL1", INTEGER);
        Expression df1 = symbol1.toSymbolReference();
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 0, 2);

        ColumnHandle column = new TestingColumnHandle("probeColumnA");
        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, df1)),
                ImmutableMap.of(symbol1, column));
        assertThat(dynamicFilter.isBlocked().isDone()).isFalse();
        assertThat(dynamicFilter.isComplete()).isFalse();
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 1L)));
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        // DynamicFilter future cancellation should not affect DynamicFilterService
        CompletableFuture<?> isBlocked = dynamicFilter.isBlocked();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(isBlocked.cancel(false)).isFalse();
        assertThat(dynamicFilter.isBlocked().isDone()).isFalse();
        assertThat(dynamicFilter.isComplete()).isFalse();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 2L)));
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(dynamicFilter.isComplete()).isTrue();
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
    }

    @Test
    public void testIsAwaitable()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
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
                ImmutableMap.of(symbol, handle));

        DynamicFilter dynamicFilter2 = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId2, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, handle));

        assertThat(dynamicFilter1.isAwaitable()).isTrue();
        // non lazy dynamic filters are marked as non-awaitable
        assertThat(dynamicFilter2.isAwaitable()).isFalse();
    }

    @Test
    public void testMultipleColumnMapping()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
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
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId1, 0, 1);

        TestingColumnHandle column1 = new TestingColumnHandle("probeColumnA");
        TestingColumnHandle column2 = new TestingColumnHandle("probeColumnB");

        DynamicFilter dynamicFilter = dynamicFilterService.createDynamicFilter(
                queryId,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, df1),
                        new DynamicFilters.Descriptor(filterId1, df2)),
                ImmutableMap.of(
                        symbol1, column1,
                        symbol2, column2));

        assertThat(dynamicFilter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(column1, column2));

        Domain domain = singleValue(INTEGER, 1L);
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId1, 0, 0),
                ImmutableMap.of(filterId1, domain));

        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                column1, domain,
                column2, domain)));
    }

    @Test
    public void testDynamicFilterConsumer()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        Set<DynamicFilterId> dynamicFilters = ImmutableSet.of(filterId1, filterId2);
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, dynamicFilters, dynamicFilters, ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 0, 2);

        Map<DynamicFilterId, Domain> consumerCollectedFilters = new HashMap<>();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                0,
                dynamicFilters,
                domains -> domains.forEach((filter, domain) -> assertThat(consumerCollectedFilters.put(filter, domain)).isNull()));
        assertThat(consumerCollectedFilters).isEmpty();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 0),
                ImmutableMap.of(filterId1, singleValue(INTEGER, 1L)));
        assertThat(consumerCollectedFilters).isEmpty();

        // complete only filterId1
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1, 0),
                ImmutableMap.of(
                        filterId1, singleValue(INTEGER, 3L),
                        filterId2, singleValue(INTEGER, 2L)));
        assertThat(consumerCollectedFilters).isEqualTo(ImmutableMap.of(filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L))));

        // register another consumer only for filterId1 after completion of filterId1
        Map<DynamicFilterId, Domain> secondConsumerCollectedFilters = new HashMap<>();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                0,
                ImmutableSet.of(filterId1),
                domains -> domains.forEach((filter, domain) -> assertThat(secondConsumerCollectedFilters.put(filter, domain)).isNull()));
        assertThat(secondConsumerCollectedFilters).isEqualTo(ImmutableMap.of(filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L))));

        // complete filterId2
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 0),
                ImmutableMap.of(filterId2, singleValue(INTEGER, 4L)));
        assertThat(consumerCollectedFilters).isEqualTo(ImmutableMap.of(
                filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L)),
                filterId2, multipleValues(INTEGER, ImmutableList.of(2L, 4L))));
        assertThat(secondConsumerCollectedFilters).isEqualTo(ImmutableMap.of(filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L))));
    }

    @Test
    public void testDynamicFilterConsumerCallbackCount()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        DynamicFilterId filterId1 = new DynamicFilterId("df1");
        DynamicFilterId filterId2 = new DynamicFilterId("df2");
        Set<DynamicFilterId> dynamicFilters = ImmutableSet.of(filterId1, filterId2);
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, dynamicFilters, dynamicFilters, ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 0, 2);

        Map<DynamicFilterId, Domain> consumerCollectedFilters = new HashMap<>();
        AtomicInteger callbackCount = new AtomicInteger();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                0,
                dynamicFilters,
                domains -> {
                    callbackCount.getAndIncrement();
                    domains.forEach((filter, domain) -> assertThat(consumerCollectedFilters.put(filter, domain)).isNull());
                });
        assertThat(consumerCollectedFilters.isEmpty()).isTrue();

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 0),
                ImmutableMap.of(
                        filterId1, singleValue(INTEGER, 1L),
                        filterId2, singleValue(INTEGER, 2L)));
        assertThat(consumerCollectedFilters.isEmpty()).isTrue();

        // complete both filterId1 and filterId2
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1, 0),
                ImmutableMap.of(
                        filterId1, singleValue(INTEGER, 3L),
                        filterId2, singleValue(INTEGER, 4L)));
        assertThat(consumerCollectedFilters).isEqualTo(ImmutableMap.of(
                filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L)),
                filterId2, multipleValues(INTEGER, ImmutableList.of(2L, 4L))));

        assertThat(callbackCount.get()).isEqualTo(2);

        // register another consumer after both filters have been collected
        Map<DynamicFilterId, Domain> secondConsumerCollectedFilters = new HashMap<>();
        AtomicInteger secondCallbackCount = new AtomicInteger();
        dynamicFilterService.registerDynamicFilterConsumer(
                queryId,
                0,
                dynamicFilters,
                domains -> {
                    secondCallbackCount.getAndIncrement();
                    domains.forEach((filter, domain) -> assertThat(secondConsumerCollectedFilters.put(filter, domain)).isNull());
                });
        assertThat(secondConsumerCollectedFilters).isEqualTo(ImmutableMap.of(
                filterId1, multipleValues(INTEGER, ImmutableList.of(1L, 3L)),
                filterId2, multipleValues(INTEGER, ImmutableList.of(2L, 4L))));
        assertThat(secondCallbackCount.get()).isEqualTo(2);
        // first consumer should not receive callback again since it already got the completed filter
        assertThat(callbackCount.get()).isEqualTo(2);
    }

    @Test
    public void testSourceStageInnerLazyDynamicFilters()
    {
        DynamicFilterId dynamicFilterId = new DynamicFilterId("filterId");
        assertThat(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, SOURCE_DISTRIBUTION, REPLICATE))).isEqualTo(ImmutableSet.of(dynamicFilterId));
        assertThat(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, FIXED_HASH_DISTRIBUTION, REPLICATE))).isEqualTo(ImmutableSet.of());
        assertThat(getSourceStageInnerLazyDynamicFilters(createPlan(dynamicFilterId, SOURCE_DISTRIBUTION, REPARTITION))).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testOutboundDynamicFilters()
    {
        DynamicFilterId filterId1 = new DynamicFilterId("filterId1");
        DynamicFilterId filterId2 = new DynamicFilterId("filterId2");
        assertThat(getOutboundDynamicFilters(createPlan(filterId1, filterId1, FIXED_HASH_DISTRIBUTION, REPLICATE))).isEqualTo(ImmutableSet.of());
        assertThat(getOutboundDynamicFilters(createPlan(filterId1, filterId1, FIXED_HASH_DISTRIBUTION, REPARTITION))).isEqualTo(ImmutableSet.of());
        assertThat(getOutboundDynamicFilters(createPlan(filterId1, filterId2, FIXED_HASH_DISTRIBUTION, REPLICATE))).isEqualTo(ImmutableSet.of(filterId1));
        assertThat(getOutboundDynamicFilters(createPlan(filterId1, filterId2, FIXED_HASH_DISTRIBUTION, REPARTITION))).isEqualTo(ImmutableSet.of(filterId1));
    }

    @Test
    public void testMultipleQueryAttempts()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        DynamicFilterId filterId = new DynamicFilterId("df");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        dynamicFilterService.registerQuery(queryId, session, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 0, 3);
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        // Collect DF from 2 tasks
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 1L)));
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 2L)));
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        // Register query retry
        dynamicFilterService.registerQueryRetry(queryId, 1);
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 1, 3);

        // Ignore update from previous attempt
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 2, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 3L)));
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        // Collect DF from 3 tasks in new attempt
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 1),
                ImmutableMap.of(filterId, singleValue(INTEGER, 4L)));
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1, 1),
                ImmutableMap.of(filterId, singleValue(INTEGER, 5L)));
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 2, 1),
                ImmutableMap.of(filterId, singleValue(INTEGER, 6L)));
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEqualTo(Optional.of(multipleValues(INTEGER, ImmutableList.of(4L, 5L, 6L))));

        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(1);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(1);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(0);
        assertThat(stats.getDynamicFilterDomainStats()).isEqualTo(ImmutableList.of(new DynamicFilterDomainStats(
                filterId,
                getSimplifiedDomainString(4L, 6L, 3, INTEGER))));
    }

    @Test
    public void testSizeLimit()
    {
        DataSize sizeLimit = DataSize.of(1, KILOBYTE);
        DynamicFilterConfig config = new DynamicFilterConfig();
        config.setEnableLargeDynamicFilters(false)
                .setSmallMaxSizePerFilter(sizeLimit);
        DynamicFilterService dynamicFilterService = new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                PLANNER_CONTEXT.getTypeOperators(),
                config);

        QueryId queryId = new QueryId("query");
        StageId stage1 = new StageId(queryId, 0);
        StageId stage2 = new StageId(queryId, 1);
        StageId stage3 = new StageId(queryId, 3);
        StageId stage4 = new StageId(queryId, 3);
        DynamicFilterId compactFilter = new DynamicFilterId("compact");
        DynamicFilterId largeFilter = new DynamicFilterId("large");
        DynamicFilterId replicatedFilter1 = new DynamicFilterId("replicated1");
        DynamicFilterId replicatedFilter2 = new DynamicFilterId("replicated2");

        dynamicFilterService.registerQuery(
                queryId,
                session,
                ImmutableSet.of(compactFilter, largeFilter, replicatedFilter1, replicatedFilter2),
                ImmutableSet.of(compactFilter, largeFilter, replicatedFilter1, replicatedFilter2),
                ImmutableSet.of(replicatedFilter1, replicatedFilter2));

        Domain domain1 = Domain.multipleValues(VARCHAR, LongStream.range(0, 5)
                .mapToObj(i -> utf8Slice("value" + i))
                .collect(toImmutableList()));
        Domain domain2 = Domain.multipleValues(VARCHAR, LongStream.range(6, 31)
                .mapToObj(i -> utf8Slice("value" + i))
                .collect(toImmutableList()));
        Domain domain3 = Domain.singleValue(VARCHAR, utf8Slice(IntStream.range(0, 800)
                .mapToObj(i -> "x")
                .collect(joining())));
        assertThat(domain1.getRetainedSizeInBytes()).isLessThan(sizeLimit.toBytes());
        assertThat(domain1.union(domain2).getRetainedSizeInBytes()).isGreaterThanOrEqualTo(sizeLimit.toBytes());
        assertThat(domain1.union(domain2).union(domain3).simplify(1).getRetainedSizeInBytes())
                .isGreaterThanOrEqualTo(sizeLimit.toBytes());

        // test filter compaction
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage1, 0, 0),
                ImmutableMap.of(compactFilter, domain1));
        assertThat(dynamicFilterService.getSummary(queryId, compactFilter)).isNotPresent();
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage1, 1, 0),
                ImmutableMap.of(compactFilter, domain2));
        assertThat(dynamicFilterService.getSummary(queryId, compactFilter)).isNotPresent();
        dynamicFilterService.stageCannotScheduleMoreTasks(stage1, 0, 2);
        assertThat(dynamicFilterService.getSummary(queryId, compactFilter)).isPresent();
        Domain compactFilterSummary = dynamicFilterService.getSummary(queryId, compactFilter).get();
        assertThat(compactFilterSummary.getValues()).isEqualTo(ValueSet.ofRanges(range(VARCHAR, utf8Slice("value0"), true, utf8Slice("value9"), true)));

        // test size limit exceeded after compaction
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage2, 0, 0),
                ImmutableMap.of(largeFilter, domain1));
        assertThat(dynamicFilterService.getSummary(queryId, largeFilter)).isNotPresent();
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage2, 1, 0),
                ImmutableMap.of(largeFilter, domain2));
        assertThat(dynamicFilterService.getSummary(queryId, largeFilter)).isNotPresent();
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage2, 2, 0),
                ImmutableMap.of(largeFilter, domain3));
        assertThat(dynamicFilterService.getSummary(queryId, largeFilter)).isPresent();
        assertThat(dynamicFilterService.getSummary(queryId, largeFilter).get()).isEqualTo(Domain.all(VARCHAR));

        // test compaction for replicated filter
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage3, 0, 0),
                ImmutableMap.of(replicatedFilter1, domain1.union(domain2)));
        assertThat(dynamicFilterService.getSummary(queryId, replicatedFilter1)).isPresent();
        assertThat(dynamicFilterService.getSummary(queryId, replicatedFilter1).get().getValues()).isEqualTo(ValueSet.ofRanges(range(VARCHAR, utf8Slice("value0"), true, utf8Slice("value9"), true)));

        // test size limit exceeded for replicated filter
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage4, 0, 0),
                ImmutableMap.of(replicatedFilter2, domain1.union(domain2).union(domain3)));
        assertThat(dynamicFilterService.getSummary(queryId, replicatedFilter2)).isPresent();
        assertThat(dynamicFilterService.getSummary(queryId, replicatedFilter2).get()).isEqualTo(Domain.all(VARCHAR));
    }

    @Test
    public void testCollectMoreThanOnceForTheSameTask()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        QueryId query = new QueryId("query");
        StageId stage = new StageId(query, 0);
        DynamicFilterId filter = new DynamicFilterId("filter");

        dynamicFilterService.registerQuery(
                query,
                session,
                ImmutableSet.of(filter),
                ImmutableSet.of(filter),
                ImmutableSet.of());

        dynamicFilterService.stageCannotScheduleMoreTasks(stage, 0, 2);

        Domain domain1 = Domain.singleValue(VARCHAR, utf8Slice("value1"));
        Domain domain2 = Domain.singleValue(VARCHAR, utf8Slice("value2"));
        Domain domain3 = Domain.singleValue(VARCHAR, utf8Slice("value3"));

        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage, 0, 0),
                ImmutableMap.of(filter, domain1));
        assertThat(dynamicFilterService.getSummary(query, filter)).isNotPresent();
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage, 0, 0),
                ImmutableMap.of(filter, domain2));
        assertThat(dynamicFilterService.getSummary(query, filter)).isNotPresent();
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stage, 1, 0),
                ImmutableMap.of(filter, domain3));
        assertThat(dynamicFilterService.getSummary(query, filter)).isPresent();
        assertThat(dynamicFilterService.getSummary(query, filter).get()).isEqualTo(domain1.union(domain3));
    }

    @Test
    public void testMultipleTaskAttempts()
    {
        DynamicFilterService dynamicFilterService = createDynamicFilterService();
        DynamicFilterId filterId = new DynamicFilterId("df");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);

        Session taskRetriesEnabled = Session.builder(session)
                .setSystemProperty(RETRY_POLICY, RetryPolicy.TASK.name())
                .build();
        dynamicFilterService.registerQuery(queryId, taskRetriesEnabled, ImmutableSet.of(filterId), ImmutableSet.of(filterId), ImmutableSet.of());
        dynamicFilterService.stageCannotScheduleMoreTasks(stageId, 0, 3);
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        // Collect DF from 2 tasks
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 1L)));
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 1, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 2L)));
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        // Collect DF from task retry of partitionId 0
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 0, 1),
                ImmutableMap.of(filterId, singleValue(INTEGER, 0L)));
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEmpty();

        // Collect DF from 3rd partition
        dynamicFilterService.addTaskDynamicFilters(
                new TaskId(stageId, 2, 0),
                ImmutableMap.of(filterId, singleValue(INTEGER, 6L)));
        // DF from task retry of partitionId 0 is ignored and the collected value from first successful attempt is kept
        assertThat(dynamicFilterService.getSummary(queryId, filterId)).isEqualTo(Optional.of(multipleValues(INTEGER, ImmutableList.of(1L, 2L, 6L))));

        DynamicFiltersStats stats = dynamicFilterService.getDynamicFilteringStats(queryId, session);
        assertThat(stats.getDynamicFiltersCompleted()).isEqualTo(1);
        assertThat(stats.getLazyDynamicFilters()).isEqualTo(1);
        assertThat(stats.getReplicatedDynamicFilters()).isEqualTo(0);
        assertThat(stats.getDynamicFilterDomainStats()).isEqualTo(ImmutableList.of(new DynamicFilterDomainStats(
                filterId,
                getSimplifiedDomainString(1L, 6L, 3, INTEGER))));
    }

    private static DynamicFilterService createDynamicFilterService()
    {
        return new DynamicFilterService(
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                PLANNER_CONTEXT.getTypeOperators(),
                new DynamicFilterConfig());
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
        Symbol symbol = new Symbol(VARCHAR, "column");
        Symbol buildSymbol = new Symbol(VARCHAR, "buildColumn");

        PlanNodeId tableScanNodeId = new PlanNodeId("plan_id");
        TableScanNode tableScan = new TableScanNode(
                tableScanNodeId,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingMetadata.TestingColumnHandle("column")),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        FilterNode filterNode = new FilterNode(
                new PlanNodeId("filter_node_id"),
                tableScan,
                createDynamicFilterExpression(createTestMetadataManager(), consumedDynamicFilterId, VARCHAR, symbol.toSymbolReference()));

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("remote_id"), new PlanFragmentId("plan_fragment_id"), ImmutableList.of(buildSymbol), Optional.empty(), exchangeType, RetryPolicy.NONE);
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
                ImmutableSet.of(symbol),
                stagePartitioning,
                Optional.empty(),
                ImmutableList.of(tableScanNodeId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }
}
