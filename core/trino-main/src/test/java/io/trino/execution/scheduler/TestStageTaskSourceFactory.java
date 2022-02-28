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
package io.trino.execution.scheduler;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.airlift.units.DataSize;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.scheduler.StageTaskSourceFactory.ArbitraryDistributionTaskSource;
import io.trino.execution.scheduler.StageTaskSourceFactory.HashDistributionTaskSource;
import io.trino.execution.scheduler.StageTaskSourceFactory.SingleDistributionTaskSource;
import io.trino.execution.scheduler.StageTaskSourceFactory.SourceDistributionTaskSource;
import io.trino.execution.scheduler.TestingExchange.TestingExchangeSourceHandle;
import io.trino.execution.scheduler.group.DynamicBucketNodeMap;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.SplitSource;
import io.trino.sql.planner.plan.PlanNodeId;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Multimaps.toMultimap;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.guava.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStageTaskSourceFactory
{
    private static final PlanNodeId PLAN_NODE_1 = new PlanNodeId("planNode1");
    private static final PlanNodeId PLAN_NODE_2 = new PlanNodeId("planNode2");
    private static final PlanNodeId PLAN_NODE_3 = new PlanNodeId("planNode3");
    private static final PlanNodeId PLAN_NODE_4 = new PlanNodeId("planNode4");
    private static final PlanNodeId PLAN_NODE_5 = new PlanNodeId("planNode5");
    private static final CatalogName CATALOG = new CatalogName("catalog");
    public static final long STANDARD_WEIGHT = SplitWeight.standard().getRawValue();

    @Test
    public void testSingleDistributionTaskSource()
    {
        ListMultimap<PlanNodeId, ExchangeSourceHandle> sources = ImmutableListMultimap.<PlanNodeId, ExchangeSourceHandle>builder()
                .put(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123))
                .put(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 321))
                .put(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 222))
                .build();
        TaskSource taskSource = new SingleDistributionTaskSource(sources);

        assertFalse(taskSource.isFinished());

        List<TaskDescriptor> tasks = taskSource.getMoreTasks();
        assertThat(tasks).hasSize(1);
        assertTrue(taskSource.isFinished());

        TaskDescriptor task = tasks.get(0);
        assertThat(task.getNodeRequirements().getCatalogName()).isEmpty();
        assertThat(task.getNodeRequirements().getAddresses()).isEmpty();
        assertEquals(task.getPartitionId(), 0);
        assertEquals(task.getExchangeSourceHandles(), sources);
        assertEquals(task.getSplits(), ImmutableListMultimap.of());
    }

    @Test
    public void testArbitraryDistributionTaskSource()
    {
        ExchangeManager splittingExchangeManager = new TestingExchangeManager(true);
        ExchangeManager nonSplittingExchangeManager = new TestingExchangeManager(false);

        TaskSource taskSource = new ArbitraryDistributionTaskSource(new IdentityHashMap<>(),
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        assertFalse(taskSource.isFinished());
        List<TaskDescriptor> tasks = taskSource.getMoreTasks();
        assertThat(tasks).isEmpty();
        assertTrue(taskSource.isFinished());

        TestingExchangeSourceHandle sourceHandle1 = new TestingExchangeSourceHandle(0, 1);
        TestingExchangeSourceHandle sourceHandle2 = new TestingExchangeSourceHandle(0, 2);
        TestingExchangeSourceHandle sourceHandle3 = new TestingExchangeSourceHandle(0, 3);
        TestingExchangeSourceHandle sourceHandle4 = new TestingExchangeSourceHandle(0, 4);
        TestingExchangeSourceHandle sourceHandle123 = new TestingExchangeSourceHandle(0, 123);
        TestingExchangeSourceHandle sourceHandle321 = new TestingExchangeSourceHandle(0, 321);
        Multimap<PlanNodeId, ExchangeSourceHandle> nonReplicatedSources = ImmutableListMultimap.of(PLAN_NODE_1, sourceHandle3);
        Exchange exchange = splittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                new IdentityHashMap<>(ImmutableMap.of(sourceHandle3, exchange)),
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(1);
        assertEquals(tasks, ImmutableList.of(new TaskDescriptor(
                0,
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 3)),
                new NodeRequirements(Optional.empty(), ImmutableSet.of()))));

        nonReplicatedSources = ImmutableListMultimap.of(PLAN_NODE_1, sourceHandle123);
        exchange = nonSplittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                new IdentityHashMap<>(ImmutableMap.of(sourceHandle123, exchange)),
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertEquals(tasks, ImmutableList.of(new TaskDescriptor(
                0,
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123)),
                new NodeRequirements(Optional.empty(), ImmutableSet.of()))));

        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle123,
                PLAN_NODE_2, sourceHandle321);
        exchange = nonSplittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                new IdentityHashMap<>(ImmutableMap.of(
                        sourceHandle123, exchange,
                        sourceHandle321, exchange)),
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertEquals(tasks, ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        1,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 321)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of()))));

        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle1,
                PLAN_NODE_1, sourceHandle2,
                PLAN_NODE_2, sourceHandle4);
        exchange = splittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                new IdentityHashMap<>(ImmutableMap.of(
                        sourceHandle1, exchange,
                        sourceHandle2, exchange,
                        sourceHandle4, exchange)),
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertEquals(tasks, ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 2)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        1,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 3)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        2,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of()))));

        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle1,
                PLAN_NODE_1, sourceHandle3,
                PLAN_NODE_2, sourceHandle4);
        exchange = splittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                new IdentityHashMap<>(ImmutableMap.of(
                        sourceHandle1, exchange,
                        sourceHandle3, exchange,
                        sourceHandle4, exchange)),
                nonReplicatedSources,
                ImmutableListMultimap.of(),
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertEquals(tasks, ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        1,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 3)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        2,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 3)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        3,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of()))));

        // with replicated sources
        nonReplicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_1, sourceHandle1,
                PLAN_NODE_1, sourceHandle2,
                PLAN_NODE_1, sourceHandle4);
        Multimap<PlanNodeId, ExchangeSourceHandle> replicatedSources = ImmutableListMultimap.of(
                PLAN_NODE_2, sourceHandle321);
        exchange = splittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                new IdentityHashMap<>(ImmutableMap.of(
                        sourceHandle1, exchange,
                        sourceHandle2, exchange,
                        sourceHandle4, exchange,
                        sourceHandle321, exchange)),
                nonReplicatedSources,
                replicatedSources,
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertEquals(tasks, ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 2),
                                PLAN_NODE_2, new TestingExchangeSourceHandle(0, 321)),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        1,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 3),
                                PLAN_NODE_2, sourceHandle321),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of())),
                new TaskDescriptor(
                        2,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                                PLAN_NODE_2, sourceHandle321),
                        new NodeRequirements(Optional.empty(), ImmutableSet.of()))));
    }

    @Test
    public void testHashDistributionTaskSource()
    {
        TaskSource taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(),
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(),
                1,
                new int[] {0, 1, 2, 3},
                Optional.empty());
        assertFalse(taskSource.isFinished());
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of());
        assertTrue(taskSource.isFinished());

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                1,
                new int[] {0, 1, 2, 3},
                Optional.empty());
        assertFalse(taskSource.isFinished());
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(0, ImmutableListMultimap.of(), ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(1, ImmutableListMultimap.of(), ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(2, ImmutableListMultimap.of(), ImmutableListMultimap.of(
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1),
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());

        Split bucketedSplit1 = createBucketedSplit(0, 0);
        Split bucketedSplit2 = createBucketedSplit(0, 2);
        Split bucketedSplit3 = createBucketedSplit(0, 3);
        Split bucketedSplit4 = createBucketedSplit(0, 1);

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(CATALOG, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(CATALOG, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                1,
                new int[] {0, 1, 2, 3},
                Optional.of(getTestingBucketNodeMap(4)));
        assertFalse(taskSource.isFinished());
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit1),
                        ImmutableListMultimap.of(
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(
                        1,
                        ImmutableListMultimap.of(
                                PLAN_NODE_5, bucketedSplit4),
                        ImmutableListMultimap.of(
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(
                        2,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit2),
                        ImmutableListMultimap.of(
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(
                        3,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit3),
                        ImmutableListMultimap.of(
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(CATALOG, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(CATALOG, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                1,
                new int[] {0, 1, 2, 3},
                Optional.of(getTestingBucketNodeMap(4)));
        assertFalse(taskSource.isFinished());
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit1),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                                PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(
                        1,
                        ImmutableListMultimap.of(
                                PLAN_NODE_5, bucketedSplit4),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(
                        2,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit2),
                        ImmutableListMultimap.of(
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(
                        3,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit3),
                        ImmutableListMultimap.of(
                                PLAN_NODE_2, new TestingExchangeSourceHandle(3, 1),
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());

        taskSource = createHashDistributionTaskSource(
                ImmutableMap.of(
                        PLAN_NODE_4, new TestingSplitSource(CATALOG, ImmutableList.of(bucketedSplit1, bucketedSplit2, bucketedSplit3)),
                        PLAN_NODE_5, new TestingSplitSource(CATALOG, ImmutableList.of(bucketedSplit4))),
                ImmutableListMultimap.of(
                        PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                        PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                        PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1)),
                ImmutableListMultimap.of(
                        PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)),
                2,
                new int[] {0, 1, 0, 1},
                Optional.of(getTestingBucketNodeMap(4)));
        assertFalse(taskSource.isFinished());
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit1,
                                PLAN_NODE_4, bucketedSplit2),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1),
                                PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1),
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())),
                new TaskDescriptor(
                        1,
                        ImmutableListMultimap.of(
                                PLAN_NODE_4, bucketedSplit3,
                                PLAN_NODE_5, bucketedSplit4),
                        ImmutableListMultimap.of(
                                PLAN_NODE_1, new TestingExchangeSourceHandle(1, 1),
                                PLAN_NODE_3, new TestingExchangeSourceHandle(0, 1)), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());
    }

    private static HashDistributionTaskSource createHashDistributionTaskSource(
            Map<PlanNodeId, SplitSource> splitSources,
            Multimap<PlanNodeId, ExchangeSourceHandle> partitionedExchangeSources,
            Multimap<PlanNodeId, ExchangeSourceHandle> replicatedExchangeSources,
            int splitBatchSize,
            int[] bucketToPartitionMap,
            Optional<BucketNodeMap> bucketNodeMap)
    {
        return new HashDistributionTaskSource(
                splitSources,
                partitionedExchangeSources,
                replicatedExchangeSources,
                splitBatchSize,
                getSplitsTime -> {},
                bucketToPartitionMap,
                bucketNodeMap,
                Optional.of(CATALOG));
    }

    @Test
    public void testSourceDistributionTaskSource()
    {
        TaskSource taskSource = createSourceDistributionTaskSource(ImmutableList.of(), ImmutableListMultimap.of(), 2, 0, 3 * STANDARD_WEIGHT, 1000);
        assertFalse(taskSource.isFinished());
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of());
        assertTrue(taskSource.isFinished());

        Split split1 = createSplit(1);
        Split split2 = createSplit(2);
        Split split3 = createSplit(3);

        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1),
                ImmutableListMultimap.of(),
                2,
                0,
                2 * STANDARD_WEIGHT,
                1000);
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(new TaskDescriptor(
                0,
                ImmutableListMultimap.of(PLAN_NODE_1, split1),
                ImmutableListMultimap.of(),
                new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());

        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1, split2, split3),
                ImmutableListMultimap.of(),
                3,
                0,
                2 * STANDARD_WEIGHT,
                1000);

        List<TaskDescriptor> tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).getSplits().values()).hasSize(2);
        assertThat(tasks.get(1).getSplits().values()).hasSize(1);
        assertThat(tasks).allMatch(taskDescriptor -> taskDescriptor.getNodeRequirements().equals(new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())));
        assertThat(tasks).allMatch(taskDescriptor -> taskDescriptor.getExchangeSourceHandles().isEmpty());
        assertThat(flattenSplits(tasks)).hasSameEntriesAs(ImmutableMultimap.of(
                PLAN_NODE_1, split1,
                PLAN_NODE_1, split2,
                PLAN_NODE_1, split3));
        assertTrue(taskSource.isFinished());

        ImmutableListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedSources = ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1));
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1, split2, split3),
                replicatedSources,
                2,
                0,
                2 * STANDARD_WEIGHT,
                1000);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).getSplits().values()).hasSize(2);
        assertThat(tasks.get(1).getSplits().values()).hasSize(1);
        assertThat(tasks).allMatch(taskDescriptor -> taskDescriptor.getNodeRequirements().equals(new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of())));
        assertThat(tasks).allMatch(taskDescriptor -> taskDescriptor.getExchangeSourceHandles().equals(replicatedSources));
        assertThat(flattenSplits(tasks)).hasSameEntriesAs(ImmutableMultimap.of(
                PLAN_NODE_1, split1,
                PLAN_NODE_1, split2,
                PLAN_NODE_1, split3));
        assertTrue(taskSource.isFinished());

        // non remotely accessible splits
        ImmutableList<Split> splits = ImmutableList.of(
                createSplit(1, "host1:8080", "host2:8080"),
                createSplit(2, "host2:8080"),
                createSplit(3, "host1:8080", "host3:8080"),
                createSplit(4, "host3:8080", "host1:8080"),
                createSplit(5, "host1:8080", "host2:8080"),
                createSplit(6, "host2:8080", "host3:8080"),
                createSplit(7, "host3:8080", "host4:8080"));
        taskSource = createSourceDistributionTaskSource(splits, ImmutableListMultimap.of(), 3, 0, 2 * STANDARD_WEIGHT, 1000);

        tasks = readAllTasks(taskSource);

        assertThat(tasks).hasSize(4);
        assertThat(tasks.stream()).allMatch(taskDescriptor -> taskDescriptor.getExchangeSourceHandles().isEmpty());
        assertThat(flattenSplits(tasks)).hasSameEntriesAs(Multimaps.index(splits, split -> PLAN_NODE_1));
        assertThat(tasks).allMatch(task -> task.getSplits().values().stream().allMatch(split -> {
            HostAddress requiredAddress = getOnlyElement(task.getNodeRequirements().getAddresses());
            return split.getAddresses().contains(requiredAddress);
        }));
        assertTrue(taskSource.isFinished());
    }

    @Test
    public void testSourceDistributionTaskSourceWithWeights()
    {
        Split split1 = createWeightedSplit(1, STANDARD_WEIGHT);
        long heavyWeight = 2 * STANDARD_WEIGHT;
        Split heavySplit1 = createWeightedSplit(11, heavyWeight);
        Split heavySplit2 = createWeightedSplit(12, heavyWeight);
        Split heavySplit3 = createWeightedSplit(13, heavyWeight);
        long lightWeight = (long) (0.5 * STANDARD_WEIGHT);
        Split lightSplit1 = createWeightedSplit(21, lightWeight);
        Split lightSplit2 = createWeightedSplit(22, lightWeight);
        Split lightSplit3 = createWeightedSplit(23, lightWeight);
        Split lightSplit4 = createWeightedSplit(24, lightWeight);

        // no limits
        TaskSource taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(lightSplit1, lightSplit2, split1, heavySplit1, heavySplit2, lightSplit4),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                (long) (1.9 * STANDARD_WEIGHT),
                1000);
        List<TaskDescriptor> tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(4);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(lightSplit1, lightSplit2, split1);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(heavySplit1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(heavySplit2);
        assertThat(tasks.get(3).getSplits().values()).containsExactlyInAnyOrder(lightSplit4); // remainder
        assertTrue(taskSource.isFinished());

        // min splits == 2
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(heavySplit1, heavySplit2, heavySplit3, lightSplit1, lightSplit2, lightSplit3, lightSplit4),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                2,
                2 * STANDARD_WEIGHT,
                1000);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(3);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(heavySplit1, heavySplit2);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(heavySplit3, lightSplit1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(lightSplit2, lightSplit3, lightSplit4);
        assertTrue(taskSource.isFinished());

        // max splits == 3
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(lightSplit1, lightSplit2, lightSplit3, heavySplit1, lightSplit4),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                2 * STANDARD_WEIGHT,
                3);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(3);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(lightSplit1, lightSplit2, lightSplit3);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(heavySplit1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(lightSplit4);
        assertTrue(taskSource.isFinished());

        // with addresses
        Split split1a1 = createWeightedSplit(1, STANDARD_WEIGHT, "host1:8080");
        Split split2a2 = createWeightedSplit(2, STANDARD_WEIGHT, "host2:8080");
        Split split3a1 = createWeightedSplit(3, STANDARD_WEIGHT, "host1:8080");
        Split split3a12 = createWeightedSplit(3, STANDARD_WEIGHT, "host1:8080", "host2:8080");
        Split heavySplit2a2 = createWeightedSplit(12, heavyWeight, "host2:8080");
        Split lightSplit1a1 = createWeightedSplit(21, lightWeight, "host1:8080");

        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1a1, heavySplit2a2, split3a1, lightSplit1a1),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                2 * STANDARD_WEIGHT,
                3);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(3);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(heavySplit2a2);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(split1a1, split3a1);
        assertThat(tasks.get(2).getSplits().values()).containsExactlyInAnyOrder(lightSplit1a1);
        assertTrue(taskSource.isFinished());

        // with addresses with multiple matching
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1a1, split3a12, split2a2),
                ImmutableListMultimap.of(),
                1, // single split per batch for predictable results
                0,
                2 * STANDARD_WEIGHT,
                3);

        tasks = readAllTasks(taskSource);
        assertThat(tasks).hasSize(2);
        assertThat(tasks).allMatch(task -> getOnlyElement(task.getSplits().keySet()).equals(PLAN_NODE_1));
        assertThat(tasks.get(0).getSplits().values()).containsExactlyInAnyOrder(split1a1, split3a12);
        assertThat(tasks.get(1).getSplits().values()).containsExactlyInAnyOrder(split2a2);
        assertTrue(taskSource.isFinished());
    }

    private static SourceDistributionTaskSource createSourceDistributionTaskSource(
            List<Split> splits,
            ListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedSources,
            int splitBatchSize,
            int minSplitsPerTask,
            long splitWeightPerTask,
            int maxSplitsPerTask)
    {
        return new SourceDistributionTaskSource(
                new QueryId("query"),
                PLAN_NODE_1,
                new TableExecuteContextManager(),
                new TestingSplitSource(CATALOG, splits),
                replicatedSources,
                splitBatchSize,
                getSplitsTime -> {},
                Optional.of(CATALOG),
                minSplitsPerTask,
                splitWeightPerTask,
                maxSplitsPerTask);
    }

    private static Split createSplit(int id, String... addresses)
    {
        return new Split(CATALOG, new TestingConnectorSplit(id, OptionalInt.empty(), addressesList(addresses)), Lifespan.taskWide());
    }

    private static Split createWeightedSplit(int id, long weight, String... addresses)
    {
        return new Split(CATALOG, new TestingConnectorSplit(id, OptionalInt.empty(), addressesList(addresses), weight), Lifespan.taskWide());
    }

    private static Split createBucketedSplit(int id, int bucket)
    {
        return new Split(CATALOG, new TestingConnectorSplit(id, OptionalInt.of(bucket), Optional.empty()), Lifespan.taskWide());
    }

    private List<TaskDescriptor> readAllTasks(TaskSource taskSource)
    {
        ImmutableList.Builder<TaskDescriptor> tasks = ImmutableList.builder();
        while (!taskSource.isFinished()) {
            tasks.addAll(taskSource.getMoreTasks());
        }
        return tasks.build();
    }

    private Multimap<PlanNodeId, Split> flattenSplits(List<TaskDescriptor> tasks)
    {
        return tasks.stream()
                .flatMap(taskDescriptor -> taskDescriptor.getSplits().entries().stream())
                .collect(toMultimap(Map.Entry::getKey, Map.Entry::getValue, HashMultimap::create));
    }

    private static Optional<List<HostAddress>> addressesList(String... addresses)
    {
        requireNonNull(addresses, "addresses is null");
        if (addresses.length == 0) {
            return Optional.empty();
        }
        return Optional.of(Arrays.stream(addresses)
                .map(HostAddress::fromString)
                .collect(toImmutableList()));
    }

    private static BucketNodeMap getTestingBucketNodeMap(int bucketCount)
    {
        return new DynamicBucketNodeMap(split -> {
            TestingConnectorSplit testingConnectorSplit = (TestingConnectorSplit) split.getConnectorSplit();
            return testingConnectorSplit.getBucket().getAsInt();
        }, bucketCount);
    }

    private static class TestingConnectorSplit
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(TestingConnectorSplit.class).instanceSize();

        private final int id;
        private final OptionalInt bucket;
        private final Optional<List<HostAddress>> addresses;
        private final SplitWeight weight;

        public TestingConnectorSplit(int id, OptionalInt bucket, Optional<List<HostAddress>> addresses)
        {
            this(id, bucket, addresses, SplitWeight.standard().getRawValue());
        }

        public TestingConnectorSplit(int id, OptionalInt bucket, Optional<List<HostAddress>> addresses, long weight)
        {
            this.id = id;
            this.bucket = requireNonNull(bucket, "bucket is null");
            this.addresses = requireNonNull(addresses, "addresses is null").map(ImmutableList::copyOf);
            this.weight = SplitWeight.fromRawValue(weight);
        }

        public int getId()
        {
            return id;
        }

        public OptionalInt getBucket()
        {
            return bucket;
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return addresses.isEmpty();
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return addresses.orElse(ImmutableList.of());
        }

        @Override
        public SplitWeight getSplitWeight()
        {
            return weight;
        }

        @Override
        public Object getInfo()
        {
            return null;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + sizeOf(bucket)
                    + sizeOf(addresses, value -> estimatedSizeOf(value, HostAddress::getRetainedSizeInBytes));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingConnectorSplit that = (TestingConnectorSplit) o;
            return id == that.id && weight == that.weight && Objects.equals(bucket, that.bucket) && Objects.equals(addresses, that.addresses);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, bucket, addresses, weight);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("bucket", bucket)
                    .add("addresses", addresses)
                    .add("weight", weight)
                    .toString();
        }
    }
}
