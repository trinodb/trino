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
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageState;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.server.DynamicFilterService.StageDynamicFilters;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.tree.Expression;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.StageState.RUNNING;
import static io.prestosql.execution.StageState.SCHEDULING;
import static io.prestosql.spi.predicate.Domain.multipleValues;
import static io.prestosql.spi.predicate.Domain.none;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterService
{
    @Test
    public void testDynamicFilterSummaryCompletion()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new TaskManagerConfig());
        DynamicFilterId filterId = new DynamicFilterId("df");
        QueryId queryId = new QueryId("query");
        StageId stageId = new StageId(queryId, 0);
        List<TaskId> taskIds = ImmutableList.of(new TaskId(stageId, 0), new TaskId(stageId, 1), new TaskId(stageId, 2));

        TestDynamicFiltersStageSupplier dynamicFiltersStageSupplier = new TestDynamicFiltersStageSupplier(RUNNING);
        dynamicFiltersStageSupplier.addTasks(taskIds);
        dynamicFilterService.registerQuery(queryId, dynamicFiltersStageSupplier, ImmutableSet.of(filterId), ImmutableSet.of(), ImmutableSet.of());
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 0),
                singleValue(INTEGER, 1L));
        dynamicFilterService.collectDynamicFilters();
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 1),
                singleValue(INTEGER, 2L));
        dynamicFilterService.collectDynamicFilters();
        assertFalse(dynamicFilterService.getSummary(queryId, filterId).isPresent());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 2);

        dynamicFiltersStageSupplier.storeSummary(
                filterId,
                new TaskId(stageId, 2),
                singleValue(INTEGER, 3L));
        dynamicFilterService.collectDynamicFilters();
        Optional<Domain> summary = dynamicFilterService.getSummary(queryId, filterId);
        assertTrue(summary.isPresent());
        assertEquals(summary.get(), multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 3);

        // all dynamic filters have been collected, no need for more requests
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 3);
    }

    @Test
    public void testDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new TaskManagerConfig());
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

        // there are still more dynamic filters to be collected
        assertFalse(dynamicFilter.isComplete());
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
        assertFalse(blockedFuture.isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 3);

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

        // there are still more dynamic filters to be collected for columns A and B
        assertFalse(dynamicFilter.isComplete());
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
        assertFalse(blockedFuture.isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 5);

        dynamicFiltersStageSupplier.storeSummary(
                filterId3,
                new TaskId(stageId3, 1),
                none(INTEGER));
        dynamicFilterService.collectDynamicFilters();

        // "none" dynamic filter (id3) has been collected for column B as tuple domains from two tasks have been provided
        assertEquals(dynamicFilter.getCurrentPredicate(), TupleDomain.none());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 6);
        assertTrue(blockedFuture.isDone());

        // all dynamic filters have been collected, no need for more requests
        dynamicFilterService.collectDynamicFilters();
        assertTrue(dynamicFilter.isComplete());
        assertTrue(dynamicFilter.isBlocked().isDone());
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 6);
    }

    @Test
    public void testReplicatedDynamicFilter()
    {
        DynamicFilterService dynamicFilterService = new DynamicFilterService(new TaskManagerConfig());
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
        assertFalse(dynamicFilter.isComplete());

        // replicated dynamic filters cannot be lazy due to replicated join task scheduling dependencies
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
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);

        // all dynamic filters have been collected, no need for more requests
        dynamicFilterService.collectDynamicFilters();
        assertEquals(dynamicFiltersStageSupplier.getRequestCount(), 1);
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
            taskIds.stream()
                    .forEach(taskId -> stageDynamicFilters
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
