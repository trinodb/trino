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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.ImmutableLongArray;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestHashDistributionSplitAssigner
{
    private static final CatalogHandle TESTING_CATALOG_HANDLE = createRootCatalogHandle("testing");

    private static final PlanNodeId PARTITIONED_1 = new PlanNodeId("partitioned-1");
    private static final PlanNodeId PARTITIONED_2 = new PlanNodeId("partitioned-2");
    private static final PlanNodeId REPLICATED_1 = new PlanNodeId("replicated-1");
    private static final PlanNodeId REPLICATED_2 = new PlanNodeId("replicated-2");

    private static final InternalNode NODE_1 = new InternalNode("node1", URI.create("http://localhost:8081"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_2 = new InternalNode("node2", URI.create("http://localhost:8082"), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_3 = new InternalNode("node3", URI.create("http://localhost:8083"), NodeVersion.UNKNOWN, false);

    @Test
    public void testEmpty()
    {
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true)),
                10,
                Optional.empty(),
                1024,
                ImmutableMap.of(),
                false,
                1);
        testAssigner(
                ImmutableSet.of(),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(new SplitBatch(REPLICATED_1, ImmutableListMultimap.of(), true)),
                1,
                Optional.empty(),
                1024,
                ImmutableMap.of(REPLICATED_1, new OutputDataSizeEstimate(ImmutableLongArray.builder().add(0).build())),
                false,
                1);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true),
                        new SplitBatch(REPLICATED_1, ImmutableListMultimap.of(), true)),
                10,
                Optional.empty(),
                1024,
                ImmutableMap.of(),
                false,
                1);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true),
                        new SplitBatch(REPLICATED_1, ImmutableListMultimap.of(), true),
                        new SplitBatch(PARTITIONED_2, ImmutableListMultimap.of(), true),
                        new SplitBatch(REPLICATED_2, ImmutableListMultimap.of(), true)),
                10,
                Optional.empty(),
                1024,
                ImmutableMap.of(),
                false,
                1);
    }

    @Test
    public void testExplicitPartitionToNodeMap()
    {
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true)),
                3,
                Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)),
                1000,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                3);
        // some partitions missing
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), true)),
                3,
                Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)),
                1000,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                1);
        // no splits
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true)),
                3,
                Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)),
                1000,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                1);
    }

    @Test
    public void testPreserveOutputPartitioning()
    {
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true)),
                3,
                Optional.empty(),
                1000,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                true,
                3);
        // some partitions missing
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), true)),
                3,
                Optional.empty(),
                1000,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                true,
                1);
        // no splits
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true)),
                3,
                Optional.empty(),
                1000,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                true,
                1);
    }

    @Test
    public void testMissingEstimates()
    {
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true)),
                3,
                Optional.empty(),
                1000,
                ImmutableMap.of(),
                false,
                3);
        // some partitions missing
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), true)),
                3,
                Optional.empty(),
                1000,
                ImmutableMap.of(),
                false,
                1);
        // no splits
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true)),
                3,
                Optional.empty(),
                1000,
                ImmutableMap.of(),
                false,
                1);
    }

    @Test
    public void testHappyPath()
    {
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true)),
                3,
                Optional.empty(),
                3,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                1);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true)),
                3,
                Optional.empty(),
                3,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                1);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true)),
                3,
                Optional.empty(),
                1,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                3);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true)),
                3,
                Optional.empty(),
                1,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                3);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(REPLICATED_2, createSplitMap(createSplit(11, 1), createSplit(12, 100)), true),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true)),
                3,
                Optional.empty(),
                1,
                ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                3);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(REPLICATED_2, createSplitMap(createSplit(11, 1), createSplit(12, 100)), true),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_2, createSplitMap(), true),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true)),
                3,
                Optional.empty(),
                1,
                ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1)),
                        PARTITIONED_2, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))),
                false,
                3);
    }

    private static void testAssigner(
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            List<SplitBatch> batches,
            int splitPartitionCount,
            Optional<List<InternalNode>> partitionToNodeMap,
            long targetPartitionSizeInBytes,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            boolean preserveOutputPartitioning,
            int expectedTaskCount)
    {
        FaultTolerantPartitioningScheme partitioningScheme = createPartitioningScheme(splitPartitionCount, partitionToNodeMap);
        HashDistributionSplitAssigner assigner = new HashDistributionSplitAssigner(
                Optional.of(TESTING_CATALOG_HANDLE),
                partitionedSources,
                replicatedSources,
                targetPartitionSizeInBytes,
                outputDataSizeEstimates,
                partitioningScheme,
                preserveOutputPartitioning);
        TestingTaskSourceCallback callback = new TestingTaskSourceCallback();
        SetMultimap<Integer, Integer> partitionedSplitIds = HashMultimap.create();
        Set<Integer> replicatedSplitIds = new HashSet<>();
        for (SplitBatch batch : batches) {
            assigner.assign(batch.getPlanNodeId(), batch.getSplits(), batch.isNoMoreSplits()).update(callback);
            boolean replicated = replicatedSources.contains(batch.getPlanNodeId());
            callback.checkContainsSplits(batch.getPlanNodeId(), batch.getSplits().values(), replicated);
            for (Map.Entry<Integer, Split> entry : batch.getSplits().entries()) {
                int splitId = TestingConnectorSplit.getSplitId(entry.getValue());
                if (replicated) {
                    assertThat(replicatedSplitIds).doesNotContain(splitId);
                    replicatedSplitIds.add(splitId);
                }
                else {
                    partitionedSplitIds.put(entry.getKey(), splitId);
                }
            }
        }
        assigner.finish().update(callback);
        List<TaskDescriptor> taskDescriptors = callback.getTaskDescriptors();
        assertThat(taskDescriptors).hasSize(expectedTaskCount);
        for (TaskDescriptor taskDescriptor : taskDescriptors) {
            int partitionId = taskDescriptor.getPartitionId();
            NodeRequirements nodeRequirements = taskDescriptor.getNodeRequirements();
            assertEquals(nodeRequirements.getCatalogHandle(), Optional.of(TESTING_CATALOG_HANDLE));
            partitionToNodeMap.ifPresent(partitionToNode -> {
                if (!taskDescriptor.getSplits().isEmpty()) {
                    InternalNode node = partitionToNode.get(partitionId);
                    assertThat(nodeRequirements.getAddresses()).containsExactly(node.getHostAndPort());
                }
            });
            Set<Integer> taskDescriptorSplitIds = taskDescriptor.getSplits().values().stream()
                    .map(TestingConnectorSplit::getSplitId)
                    .collect(toImmutableSet());
            assertThat(taskDescriptorSplitIds).containsAll(replicatedSplitIds);
            Set<Integer> taskDescriptorPartitionedSplitIds = difference(taskDescriptorSplitIds, replicatedSplitIds);
            Set<Integer> taskDescriptorSplitPartitions = new HashSet<>();
            for (Split split : taskDescriptor.getSplits().values()) {
                int splitId = TestingConnectorSplit.getSplitId(split);
                if (taskDescriptorPartitionedSplitIds.contains(splitId)) {
                    int splitPartition = partitioningScheme.getPartition(split);
                    taskDescriptorSplitPartitions.add(splitPartition);
                }
            }
            for (Integer splitPartition : taskDescriptorSplitPartitions) {
                assertThat(taskDescriptorPartitionedSplitIds).containsAll(partitionedSplitIds.get(splitPartition));
            }
        }
    }

    private static ListMultimap<Integer, Split> createSplitMap(Split... splits)
    {
        return Arrays.stream(splits)
                .collect(toImmutableListMultimap(split -> ((TestingConnectorSplit) split.getConnectorSplit()).getBucket().orElseThrow(), Function.identity()));
    }

    private static FaultTolerantPartitioningScheme createPartitioningScheme(int partitionCount, Optional<List<InternalNode>> partitionToNodeMap)
    {
        return new FaultTolerantPartitioningScheme(
                partitionCount,
                Optional.of(IntStream.range(0, partitionCount).toArray()),
                Optional.of(split -> ((TestingConnectorSplit) split.getConnectorSplit()).getBucket().orElseThrow()),
                partitionToNodeMap);
    }

    private static Split createSplit(int id, int partition)
    {
        return new Split(TESTING_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.of(partition), Optional.empty()));
    }

    private static class SplitBatch
    {
        private final PlanNodeId planNodeId;
        private final ListMultimap<Integer, Split> splits;
        private final boolean noMoreSplits;

        public SplitBatch(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.splits = ImmutableListMultimap.copyOf(requireNonNull(splits, "splits is null"));
            this.noMoreSplits = noMoreSplits;
        }

        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        public ListMultimap<Integer, Split> getSplits()
        {
            return splits;
        }

        public boolean isNoMoreSplits()
        {
            return noMoreSplits;
        }
    }
}
