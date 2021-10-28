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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.SplitSource;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStageTaskSourceFactory
{
    private static final PlanFragmentId FRAGMENT_ID_1 = new PlanFragmentId("1");
    private static final PlanFragmentId FRAGMENT_ID_2 = new PlanFragmentId("2");
    private static final PlanNodeId PLAN_NODE_1 = new PlanNodeId("planNode1");
    private static final PlanNodeId PLAN_NODE_2 = new PlanNodeId("planNode2");
    private static final PlanNodeId PLAN_NODE_3 = new PlanNodeId("planNode3");
    private static final PlanNodeId PLAN_NODE_4 = new PlanNodeId("planNode4");
    private static final PlanNodeId PLAN_NODE_5 = new PlanNodeId("planNode5");
    private static final CatalogName CATALOG = new CatalogName("catalog");

    @Test
    public void testSingleDistributionTaskSource()
    {
        Multimap<PlanNodeId, ExchangeSourceHandle> sources = ImmutableListMultimap.<PlanNodeId, ExchangeSourceHandle>builder()
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

        TaskSource taskSource = new ArbitraryDistributionTaskSource(ImmutableMap.of(), ImmutableMap.of(), ImmutableListMultimap.of(), DataSize.of(3, BYTE));
        assertFalse(taskSource.isFinished());
        List<TaskDescriptor> tasks = taskSource.getMoreTasks();
        assertThat(tasks).isEmpty();
        assertTrue(taskSource.isFinished());

        Multimap<PlanFragmentId, ExchangeSourceHandle> sources = ImmutableListMultimap.of(FRAGMENT_ID_1, new TestingExchangeSourceHandle(0, 3));
        Exchange exchange = splittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), 0), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                ImmutableMap.of(FRAGMENT_ID_1, PLAN_NODE_1),
                ImmutableMap.of(FRAGMENT_ID_1, exchange),
                sources,
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertTrue(taskSource.isFinished());
        assertThat(tasks).hasSize(1);
        assertEquals(tasks, ImmutableList.of(new TaskDescriptor(
                0,
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 3)),
                new NodeRequirements(Optional.empty(), ImmutableSet.of()))));

        sources = ImmutableListMultimap.of(FRAGMENT_ID_1, new TestingExchangeSourceHandle(0, 123));
        exchange = nonSplittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), 0), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                ImmutableMap.of(FRAGMENT_ID_1, PLAN_NODE_1),
                ImmutableMap.of(FRAGMENT_ID_1, exchange),
                sources,
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertEquals(tasks, ImmutableList.of(new TaskDescriptor(
                0,
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 123)),
                new NodeRequirements(Optional.empty(), ImmutableSet.of()))));

        sources = ImmutableListMultimap.of(
                FRAGMENT_ID_1, new TestingExchangeSourceHandle(0, 123),
                FRAGMENT_ID_2, new TestingExchangeSourceHandle(0, 321));
        exchange = nonSplittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), 0), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                ImmutableMap.of(FRAGMENT_ID_1, PLAN_NODE_1, FRAGMENT_ID_2, PLAN_NODE_2),
                ImmutableMap.of(FRAGMENT_ID_1, exchange, FRAGMENT_ID_2, exchange),
                sources,
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

        sources = ImmutableListMultimap.of(
                FRAGMENT_ID_1, new TestingExchangeSourceHandle(0, 1),
                FRAGMENT_ID_1, new TestingExchangeSourceHandle(0, 2),
                FRAGMENT_ID_2, new TestingExchangeSourceHandle(0, 4));
        exchange = splittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), 0), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                ImmutableMap.of(FRAGMENT_ID_1, PLAN_NODE_1, FRAGMENT_ID_2, PLAN_NODE_2),
                ImmutableMap.of(FRAGMENT_ID_1, exchange, FRAGMENT_ID_2, exchange),
                sources,
                DataSize.of(3, BYTE));
        tasks = taskSource.getMoreTasks();
        assertEquals(tasks, ImmutableList.of(
                new TaskDescriptor(
                        0,
                        ImmutableListMultimap.of(),
                        ImmutableListMultimap.of(PLAN_NODE_1, new TestingExchangeSourceHandle(0, 1), PLAN_NODE_1, new TestingExchangeSourceHandle(0, 2)),
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

        sources = ImmutableListMultimap.of(
                FRAGMENT_ID_1, new TestingExchangeSourceHandle(0, 1),
                FRAGMENT_ID_1, new TestingExchangeSourceHandle(0, 3),
                FRAGMENT_ID_2, new TestingExchangeSourceHandle(0, 4));
        exchange = splittingExchangeManager.createExchange(new ExchangeContext(new QueryId("query"), 0), 3);
        taskSource = new ArbitraryDistributionTaskSource(
                ImmutableMap.of(FRAGMENT_ID_1, PLAN_NODE_1, FRAGMENT_ID_2, PLAN_NODE_2),
                ImmutableMap.of(FRAGMENT_ID_1, exchange, FRAGMENT_ID_2, exchange),
                sources,
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
                (getSplitsTime) -> {},
                bucketToPartitionMap,
                bucketNodeMap,
                Optional.of(CATALOG));
    }

    @Test
    public void testSourceDistributionTaskSource()
    {
        TaskSource taskSource = createSourceDistributionTaskSource(ImmutableList.of(), ImmutableListMultimap.of(), 2, 3);
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
                2);
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
                2);
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(0, ImmutableListMultimap.of(PLAN_NODE_1, split1, PLAN_NODE_1, split2), ImmutableListMultimap.of(), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(1, ImmutableListMultimap.of(PLAN_NODE_1, split3), ImmutableListMultimap.of(), new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());

        ImmutableListMultimap<PlanNodeId, ExchangeSourceHandle> replicatedSources = ImmutableListMultimap.of(PLAN_NODE_2, new TestingExchangeSourceHandle(0, 1));
        taskSource = createSourceDistributionTaskSource(
                ImmutableList.of(split1, split2, split3),
                replicatedSources,
                2,
                2);
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(0, ImmutableListMultimap.of(PLAN_NODE_1, split1, PLAN_NODE_1, split2), replicatedSources, new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertFalse(taskSource.isFinished());
        assertEquals(taskSource.getMoreTasks(), ImmutableList.of(
                new TaskDescriptor(1, ImmutableListMultimap.of(PLAN_NODE_1, split3), replicatedSources, new NodeRequirements(Optional.of(CATALOG), ImmutableSet.of()))));
        assertTrue(taskSource.isFinished());

        // non remotely accessible splits
        ImmutableList<Split> splits = ImmutableList.of(
                createSplit(1, ImmutableList.of(HostAddress.fromString("host1:8080"), HostAddress.fromString("host2:8080"))),
                createSplit(2, ImmutableList.of(HostAddress.fromString("host2:8080"))),
                createSplit(3, ImmutableList.of(HostAddress.fromString("host1:8080"), HostAddress.fromString("host3:8080"))),
                createSplit(4, ImmutableList.of(HostAddress.fromString("host3:8080"), HostAddress.fromString("host1:8080"))),
                createSplit(5, ImmutableList.of(HostAddress.fromString("host1:8080"), HostAddress.fromString("host2:8080"))),
                createSplit(6, ImmutableList.of(HostAddress.fromString("host2:8080"), HostAddress.fromString("host3:8080"))),
                createSplit(7, ImmutableList.of(HostAddress.fromString("host3:8080"), HostAddress.fromString("host4:8080"))));
        taskSource = createSourceDistributionTaskSource(splits, ImmutableListMultimap.of(), 3, 2);

        List<TaskDescriptor> tasks = taskSource.getMoreTasks();
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.get(0).getNodeRequirements().getAddresses(), ImmutableSet.of(HostAddress.fromString("host1:8080")));
        assertThat(tasks.get(0).getSplits().get(PLAN_NODE_1)).containsExactlyInAnyOrder(splits.get(0), splits.get(2));
        assertFalse(taskSource.isFinished());

        tasks = taskSource.getMoreTasks();
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.get(0).getNodeRequirements().getAddresses(), ImmutableSet.of(HostAddress.fromString("host1:8080")));
        assertThat(tasks.get(0).getSplits().get(PLAN_NODE_1)).containsExactlyInAnyOrder(splits.get(3), splits.get(4));
        assertFalse(taskSource.isFinished());

        tasks = taskSource.getMoreTasks();
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.get(0).getNodeRequirements().getAddresses(), ImmutableSet.of(HostAddress.fromString("host2:8080")));
        assertThat(tasks.get(0).getSplits().get(PLAN_NODE_1)).containsExactlyInAnyOrder(splits.get(1), splits.get(5));
        assertFalse(taskSource.isFinished());

        tasks = taskSource.getMoreTasks();
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.get(0).getNodeRequirements().getAddresses(), ImmutableSet.of(HostAddress.fromString("host3:8080")));
        assertThat(tasks.get(0).getSplits().get(PLAN_NODE_1)).containsExactlyInAnyOrder(splits.get(6));
        assertTrue(taskSource.isFinished());
    }

    private static SourceDistributionTaskSource createSourceDistributionTaskSource(
            List<Split> splits,
            Multimap<PlanNodeId, ExchangeSourceHandle> replicatedSources,
            int splitBatchSize,
            int splitsPerTask)
    {
        return new SourceDistributionTaskSource(
                new QueryId("query"),
                PLAN_NODE_1,
                new TableExecuteContextManager(),
                new TestingSplitSource(CATALOG, splits),
                replicatedSources,
                splitBatchSize,
                (getSplitsTime) -> {},
                Optional.of(CATALOG),
                splitsPerTask);
    }

    private static Split createSplit(int id)
    {
        return new Split(CATALOG, new TestingConnectorSplit(id, OptionalInt.empty(), Optional.empty()), Lifespan.taskWide());
    }

    private static Split createSplit(int id, List<HostAddress> addresses)
    {
        return new Split(CATALOG, new TestingConnectorSplit(id, OptionalInt.empty(), Optional.of(addresses)), Lifespan.taskWide());
    }

    private static Split createBucketedSplit(int id, int bucket)
    {
        return createBucketedSplit(id, bucket, Optional.empty());
    }

    private static Split createBucketedSplit(int id, int bucket, Optional<List<HostAddress>> addresses)
    {
        return new Split(CATALOG, new TestingConnectorSplit(id, OptionalInt.of(bucket), addresses), Lifespan.taskWide());
    }

    private static BucketNodeMap getTestingBucketNodeMap(int bucketCount)
    {
        return new DynamicBucketNodeMap((split) -> {
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

        public TestingConnectorSplit(int id, OptionalInt bucket, Optional<List<HostAddress>> addresses)
        {
            this.id = id;
            this.bucket = requireNonNull(bucket, "bucket is null");
            this.addresses = requireNonNull(addresses, "addresses is null").map(ImmutableList::copyOf);
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
            return id == that.id && Objects.equals(bucket, that.bucket) && Objects.equals(addresses, that.addresses);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, bucket, addresses);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("bucket", bucket)
                    .add("addresses", addresses)
                    .toString();
        }
    }
}
