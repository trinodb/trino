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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.ImmutableLongArray;
import io.trino.client.NodeVersion;
import io.trino.execution.scheduler.OutputDataSizeEstimate;
import io.trino.execution.scheduler.faulttolerant.HashDistributionSplitAssigner.TaskPartition;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static io.trino.execution.scheduler.faulttolerant.HashDistributionSplitAssigner.createSourcePartitionToTaskPartition;
import static io.trino.execution.scheduler.faulttolerant.SplitAssigner.SINGLE_SOURCE_PARTITION_ID;
import static io.trino.execution.scheduler.faulttolerant.TestingConnectorSplit.getSplitId;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class TestHashDistributionSplitAssigner
{
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
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true))
                .withSplitPartitionCount(10)
                .withTargetPartitionSizeInBytes(1024)
                .withMergeAllowed(true)
                .withExpectedTaskCount(10)
                .run();
        testAssigner()
                .withReplicatedSources(REPLICATED_1)
                .withSplits(new SplitBatch(REPLICATED_1, ImmutableListMultimap.of(), true))
                .withSplitPartitionCount(1)
                .withTargetPartitionSizeInBytes(1024)
                .withSourceDataSizeEstimates(ImmutableMap.of(REPLICATED_1, new OutputDataSizeEstimate(ImmutableLongArray.builder().add(0).build())))
                .withMergeAllowed(true)
                .withExpectedTaskCount(1)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withReplicatedSources(REPLICATED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true),
                        new SplitBatch(REPLICATED_1, ImmutableListMultimap.of(), true))
                .withSplitPartitionCount(10)
                .withTargetPartitionSizeInBytes(1024)
                .withMergeAllowed(true)
                .withExpectedTaskCount(10)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1, PARTITIONED_2)
                .withReplicatedSources(REPLICATED_1, REPLICATED_2)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true),
                        new SplitBatch(REPLICATED_1, ImmutableListMultimap.of(), true),
                        new SplitBatch(PARTITIONED_2, ImmutableListMultimap.of(), true),
                        new SplitBatch(REPLICATED_2, ImmutableListMultimap.of(), true))
                .withSplitPartitionCount(10)
                .withTargetPartitionSizeInBytes(1024)
                .withMergeAllowed(true)
                .withExpectedTaskCount(10)
                .run();
    }

    @Test
    public void testExplicitPartitionToNodeMap()
    {
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true))
                .withSplitPartitionCount(3)
                .withPartitionToNodeMap(Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)))
                .withTargetPartitionSizeInBytes(1000)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        // some partitions missing
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), true))
                .withSplitPartitionCount(3)
                .withPartitionToNodeMap(Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)))
                .withTargetPartitionSizeInBytes(1000)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        // no splits
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true))
                .withSplitPartitionCount(3)
                .withPartitionToNodeMap(Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)))
                .withTargetPartitionSizeInBytes(1000)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
    }

    @Test
    public void testMergeNotAllowed()
    {
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(1000)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(false)
                .withExpectedTaskCount(3)
                .run();
        // some partitions missing
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(1000)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(false)
                .withExpectedTaskCount(3)
                .run();
        // no splits
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(1000)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(false)
                .withExpectedTaskCount(3)
                .run();
    }

    @Test
    public void testMissingEstimates()
    {
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true))
                .withSplitPartitionCount(3)
                .withPartitionToNodeMap(Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)))
                .withTargetPartitionSizeInBytes(1000)
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        // some partitions missing
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), true))
                .withSplitPartitionCount(3)
                .withPartitionToNodeMap(Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)))
                .withTargetPartitionSizeInBytes(1000)
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        // no splits
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, ImmutableListMultimap.of(), true))
                .withSplitPartitionCount(3)
                .withPartitionToNodeMap(Optional.of(ImmutableList.of(NODE_1, NODE_2, NODE_3)))
                .withTargetPartitionSizeInBytes(1000)
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
    }

    @Test
    public void testHappyPath()
    {
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 1), createSplit(3, 2)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(3)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(1)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withReplicatedSources(REPLICATED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(3)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(1)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withReplicatedSources(REPLICATED_1)
                .withSplits(
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(1)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withReplicatedSources(REPLICATED_1)
                .withSplits(
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(1)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withReplicatedSources(REPLICATED_1, REPLICATED_2)
                .withSplits(
                        new SplitBatch(REPLICATED_2, createSplitMap(createSplit(11, 1), createSplit(12, 100)), true),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(1)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1, PARTITIONED_2)
                .withReplicatedSources(REPLICATED_1, REPLICATED_2)
                .withSplits(
                        new SplitBatch(REPLICATED_2, createSplitMap(createSplit(11, 1), createSplit(12, 100)), true),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(2, 0), createSplit(3, 2)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_2, createSplitMap(), true),
                        new SplitBatch(REPLICATED_1, createSplitMap(createSplit(4, 1), createSplit(5, 100)), true),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(6, 1), createSplit(7, 2)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1)),
                        PARTITIONED_2, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
    }

    @Test
    public void testPartitionSplitting()
    {
        // single splittable source
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 0), createSplit(3, 0)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(3)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(5, 1, 1))))
                .withSplittableSources(PARTITIONED_1)
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();

        // largest source is not splittable
        testAssigner()
                .withPartitionedSources(PARTITIONED_1)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 0), createSplit(3, 0)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(3)
                .withSourceDataSizeEstimates(ImmutableMap.of(PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(5, 1, 1))))
                .withMergeAllowed(true)
                .withExpectedTaskCount(2)
                .run();

        // multiple sources
        testAssigner()
                .withPartitionedSources(PARTITIONED_1, PARTITIONED_2)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 0), createSplit(3, 0)), true),
                        new SplitBatch(PARTITIONED_2, createSplitMap(createSplit(4, 0), createSplit(5, 1)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(30)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(50, 1, 1)),
                        PARTITIONED_2, new OutputDataSizeEstimate(ImmutableLongArray.of(2, 1, 1))))
                .withSplittableSources(PARTITIONED_1)
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1, PARTITIONED_2)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 0), createSplit(3, 0)), true),
                        new SplitBatch(PARTITIONED_2, createSplitMap(createSplit(4, 0), createSplit(5, 1)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(30)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(50, 1, 1)),
                        PARTITIONED_2, new OutputDataSizeEstimate(ImmutableLongArray.of(2, 1, 1))))
                .withSplittableSources(PARTITIONED_1, PARTITIONED_2)
                .withMergeAllowed(true)
                .withExpectedTaskCount(3)
                .run();
        testAssigner()
                .withPartitionedSources(PARTITIONED_1, PARTITIONED_2)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 0), createSplit(3, 0)), true),
                        new SplitBatch(PARTITIONED_2, createSplitMap(createSplit(4, 0), createSplit(5, 0)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(30)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(50, 1, 1)),
                        PARTITIONED_2, new OutputDataSizeEstimate(ImmutableLongArray.of(2, 1, 1))))
                .withSplittableSources(PARTITIONED_2)
                .withMergeAllowed(true)
                .withExpectedTaskCount(2)
                .run();

        // targetPartitionSizeInBytes re-adjustment based on taskTargetMaxCount
        testAssigner()
                .withPartitionedSources(PARTITIONED_1, PARTITIONED_2)
                .withSplits(
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(0, 0), createSplit(1, 0)), false),
                        new SplitBatch(PARTITIONED_1, createSplitMap(createSplit(2, 0), createSplit(3, 0)), true),
                        new SplitBatch(PARTITIONED_2, createSplitMap(createSplit(4, 0), createSplit(5, 1)), true))
                .withSplitPartitionCount(3)
                .withTargetPartitionSizeInBytes(30)
                .withTaskTargetMaxCount(10)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1000, 1, 1)),
                        PARTITIONED_2, new OutputDataSizeEstimate(ImmutableLongArray.of(2, 1, 1))))
                .withSplittableSources(PARTITIONED_1, PARTITIONED_2)
                .withMergeAllowed(true)
                .withExpectedTaskCount(12)
                .run();
    }

    @Test
    public void testCreateOutputPartitionToTaskPartition()
    {
        testPartitionMapping()
                .withSplitPartitionCount(3)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(50, 1, 1))))
                .withTargetPartitionSizeInBytes(25)
                .withSplittableSources(PARTITIONED_1)
                .withMergeAllowed(true)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0), 3),
                        new PartitionMapping(ImmutableSet.of(1, 2), 1))
                .run();
        testPartitionMapping()
                .withSplitPartitionCount(3)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(50, 1, 1))))
                .withTargetPartitionSizeInBytes(25)
                .withMergeAllowed(true)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0), 1),
                        new PartitionMapping(ImmutableSet.of(1, 2), 1))
                .run();
        testPartitionMapping()
                .withSplitPartitionCount(3)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(50, 1, 1))))
                .withTargetPartitionSizeInBytes(25)
                .withMergeAllowed(false)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0), 1),
                        new PartitionMapping(ImmutableSet.of(1), 1),
                        new PartitionMapping(ImmutableSet.of(2), 1))
                .run();
        testPartitionMapping()
                .withSplitPartitionCount(3)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(50, 1, 1))))
                .withTargetPartitionSizeInBytes(25)
                .withMergeAllowed(false)
                .withSplittableSources(PARTITIONED_1)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0), 3),
                        new PartitionMapping(ImmutableSet.of(1), 1),
                        new PartitionMapping(ImmutableSet.of(2), 1))
                .run();
        testPartitionMapping()
                .withSplitPartitionCount(4)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(0, 0, 0, 60))))
                .withTargetPartitionSizeInBytes(25)
                .withMergeAllowed(false)
                .withSplittableSources(PARTITIONED_1)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0), 1),
                        new PartitionMapping(ImmutableSet.of(1), 1),
                        new PartitionMapping(ImmutableSet.of(2), 1),
                        new PartitionMapping(ImmutableSet.of(3), 3))
                .run();
    }

    @Test
    public void testCreateOutputPartitionToTaskPartitionWithMinTaskCount()
    {
        // without enforcing minTaskCount we should get only 2 tasks
        testPartitionMapping()
                .withSplitPartitionCount(8)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(10, 10, 10, 10, 10, 10, 10, 10))))
                .withTargetPartitionSizeInBytes(50)
                .withMergeAllowed(true)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0, 1, 2, 3, 4), 1),
                        new PartitionMapping(ImmutableSet.of(5, 6, 7), 1))
                .run();

        // enforce at least 4 tasks
        testPartitionMapping()
                .withSplitPartitionCount(8)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(10, 10, 10, 10, 10, 10, 10, 10))))
                .withTargetPartitionSizeInBytes(50)
                .withMergeAllowed(true)
                .withTargetMinTaskCount(4)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0, 1), 1),
                        new PartitionMapping(ImmutableSet.of(2, 3), 1),
                        new PartitionMapping(ImmutableSet.of(4, 5), 1),
                        new PartitionMapping(ImmutableSet.of(6, 7), 1))
                .run();

        // skewed partitions sizes - no minTaskCount enforcement
        testPartitionMapping()
                .withSplitPartitionCount(8)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 10, 1, 1, 1, 1, 1))))
                .withTargetPartitionSizeInBytes(50)
                .withMergeAllowed(true)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7), 1))
                .run();

        // skewed partitions sizes - request at least 4 tasks
        // with skew it is expected that we are getting 3 as minTaskCount is only used to compute target partitionSize
        testPartitionMapping()
                .withSplitPartitionCount(8)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(1, 1, 10, 1, 1, 1, 1, 1))))
                .withTargetPartitionSizeInBytes(50)
                .withMergeAllowed(true)
                .withTargetMinTaskCount(4)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0, 1, 3, 4), 1),
                        new PartitionMapping(ImmutableSet.of(2), 1),
                        new PartitionMapping(ImmutableSet.of(5, 6, 7), 1))
                .run();

        // 2 partitions merged
        testPartitionMapping()
                .withSplitPartitionCount(2)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(10, 10))))
                .withTargetPartitionSizeInBytes(50)
                .withMergeAllowed(true)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0, 1), 1))
                .run();

        // request 4 tasks when we only have 2 partitions only gets us 2 tasks.
        testPartitionMapping()
                .withSplitPartitionCount(2)
                .withPartitionedSources(PARTITIONED_1)
                .withSourceDataSizeEstimates(ImmutableMap.of(
                        PARTITIONED_1, new OutputDataSizeEstimate(ImmutableLongArray.of(10, 10))))
                .withTargetPartitionSizeInBytes(50)
                .withMergeAllowed(true)
                .withTargetMinTaskCount(4)
                .withExpectedMappings(
                        new PartitionMapping(ImmutableSet.of(0), 1),
                        new PartitionMapping(ImmutableSet.of(1), 1))
                .run();
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
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.of(partition), Optional.empty()));
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

    public static AssignerTester testAssigner()
    {
        return new AssignerTester();
    }

    private static class AssignerTester
    {
        private Set<PlanNodeId> partitionedSources = ImmutableSet.of();
        private Set<PlanNodeId> replicatedSources = ImmutableSet.of();
        private List<SplitBatch> splits = ImmutableList.of();
        private int splitPartitionCount;
        private Optional<List<InternalNode>> partitionToNodeMap = Optional.empty();
        private long targetPartitionSizeInBytes;
        private int taskTargetMinCount;
        private int taskTargetMaxCount = Integer.MAX_VALUE;
        private Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates = ImmutableMap.of();
        private Set<PlanNodeId> splittableSources = ImmutableSet.of();
        private boolean mergeAllowed;
        private int expectedTaskCount;

        public AssignerTester withPartitionedSources(PlanNodeId... sources)
        {
            partitionedSources = ImmutableSet.copyOf(sources);
            return this;
        }

        public AssignerTester withReplicatedSources(PlanNodeId... sources)
        {
            replicatedSources = ImmutableSet.copyOf(sources);
            return this;
        }

        public AssignerTester withSplits(SplitBatch... splits)
        {
            this.splits = ImmutableList.copyOf(splits);
            return this;
        }

        public AssignerTester withSplitPartitionCount(int splitPartitionCount)
        {
            this.splitPartitionCount = splitPartitionCount;
            return this;
        }

        public AssignerTester withPartitionToNodeMap(Optional<List<InternalNode>> partitionToNodeMap)
        {
            this.partitionToNodeMap = partitionToNodeMap;
            return this;
        }

        public AssignerTester withTargetPartitionSizeInBytes(long targetPartitionSizeInBytes)
        {
            this.targetPartitionSizeInBytes = targetPartitionSizeInBytes;
            return this;
        }

        public AssignerTester withTaskTargetMaxCount(int taskTargetMaxCount)
        {
            this.taskTargetMaxCount = taskTargetMaxCount;
            return this;
        }

        public AssignerTester withSourceDataSizeEstimates(Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates)
        {
            this.sourceDataSizeEstimates = sourceDataSizeEstimates;
            return this;
        }

        public AssignerTester withSplittableSources(PlanNodeId... sources)
        {
            splittableSources = ImmutableSet.copyOf(sources);
            return this;
        }

        public AssignerTester withMergeAllowed(boolean mergeAllowed)
        {
            this.mergeAllowed = mergeAllowed;
            return this;
        }

        public AssignerTester withExpectedTaskCount(int expectedTaskCount)
        {
            this.expectedTaskCount = expectedTaskCount;
            return this;
        }

        public void run()
        {
            FaultTolerantPartitioningScheme partitioningScheme = createPartitioningScheme(splitPartitionCount, partitionToNodeMap);
            Map<Integer, TaskPartition> sourcePartitionToTaskPartition = createSourcePartitionToTaskPartition(
                    partitioningScheme,
                    partitionedSources,
                    sourceDataSizeEstimates,
                    targetPartitionSizeInBytes,
                    taskTargetMinCount,
                    taskTargetMaxCount,
                    splittableSources::contains,
                    mergeAllowed);
            HashDistributionSplitAssigner assigner = new HashDistributionSplitAssigner(
                    new PlanFragmentId("fragment"),
                    Optional.of(TEST_CATALOG_HANDLE),
                    partitionedSources,
                    replicatedSources,
                    partitioningScheme,
                    sourcePartitionToTaskPartition);
            SplitAssignerTester tester = new SplitAssignerTester();
            Map<Integer, ListMultimap<PlanNodeId, Integer>> partitionedSplitIds = new HashMap<>();
            Multimap<PlanNodeId, Integer> replicatedSplitIds = HashMultimap.create();
            for (SplitBatch batch : splits) {
                tester.update(assigner.assign(batch.getPlanNodeId(), batch.getSplits(), batch.isNoMoreSplits()));
                boolean replicated = replicatedSources.contains(batch.getPlanNodeId());
                tester.checkContainsSplits(batch.getPlanNodeId(), batch.getSplits(), replicated);
                for (Map.Entry<Integer, Split> entry : batch.getSplits().entries()) {
                    int splitId = getSplitId(entry.getValue());
                    if (replicated) {
                        assertThat(replicatedSplitIds.containsValue(splitId)).isFalse();
                        replicatedSplitIds.put(batch.getPlanNodeId(), splitId);
                    }
                    else {
                        partitionedSplitIds.computeIfAbsent(entry.getKey(), key -> ArrayListMultimap.create()).put(batch.getPlanNodeId(), splitId);
                    }
                }
            }
            tester.update(assigner.finish());
            Map<Integer, TaskDescriptor> taskDescriptors = tester.getTaskDescriptors().orElseThrow().stream()
                    .collect(toImmutableMap(TaskDescriptor::getPartitionId, Function.identity()));
            assertThat(taskDescriptors).hasSize(expectedTaskCount);

            // validate node requirements and replicated splits
            for (TaskDescriptor taskDescriptor : taskDescriptors.values()) {
                int partitionId = taskDescriptor.getPartitionId();
                NodeRequirements nodeRequirements = taskDescriptor.getNodeRequirements();
                assertThat(nodeRequirements.getCatalogHandle()).isEqualTo(Optional.of(TEST_CATALOG_HANDLE));
                partitionToNodeMap.ifPresent(partitionToNode -> {
                    if (!taskDescriptor.getSplits().getSplitsFlat().isEmpty()) {
                        InternalNode node = partitionToNode.get(partitionId);
                        assertThat(nodeRequirements.getAddress()).hasValue(node.getHostAndPort());
                    }
                });
                Set<Integer> taskDescriptorSplitIds = new HashSet<>();
                replicatedSplitIds.keySet().forEach(planNodeId -> {
                    // all replicated splits should be assigned to single source partition in task descriptor
                    taskDescriptor.getSplits().getSplits(planNodeId).get(SINGLE_SOURCE_PARTITION_ID).stream()
                            .map(TestingConnectorSplit::getSplitId)
                            .forEach(taskDescriptorSplitIds::add);
                });
                assertThat(taskDescriptorSplitIds).containsAll(replicatedSplitIds.values());
            }

            // validate partitioned splits
            partitionedSplitIds.forEach((partitionId, sourceSplits) -> {
                sourceSplits.forEach((source, splitId) -> {
                    List<TaskDescriptor> descriptors = sourcePartitionToTaskPartition.get(partitionId).getSubPartitions().stream()
                            .filter(HashDistributionSplitAssigner.SubPartition::isIdAssigned)
                            .map(HashDistributionSplitAssigner.SubPartition::getId)
                            .map(taskDescriptors::get)
                            .collect(toImmutableList());
                    for (TaskDescriptor descriptor : descriptors) {
                        Multimap<Integer, Integer> taskDescriptorSplitIds = descriptor.getSplits().getSplits(source).entries().stream()
                                .collect(toImmutableListMultimap(
                                        Map.Entry::getKey,
                                        entry -> getSplitId(entry.getValue())));

                        if (taskDescriptorSplitIds.get(partitionId).contains(splitId) && splittableSources.contains(source)) {
                            return;
                        }
                        if (!taskDescriptorSplitIds.get(partitionId).contains(splitId) && !splittableSources.contains(source)) {
                            fail("expected split not found: ." + splitId);
                        }
                    }
                    if (splittableSources.contains(source)) {
                        fail("expected split not found: ." + splitId);
                    }
                });
            });
        }
    }

    private static PartitionMappingTester testPartitionMapping()
    {
        return new PartitionMappingTester();
    }

    private static class PartitionMappingTester
    {
        private Set<PlanNodeId> partitionedSources = ImmutableSet.of();
        private int splitPartitionCount;
        private Optional<List<InternalNode>> partitionToNodeMap = Optional.empty();
        private long targetPartitionSizeInBytes;
        private int targetMinTaskCount;
        private Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates = ImmutableMap.of();
        private Set<PlanNodeId> splittableSources = ImmutableSet.of();
        private boolean mergeAllowed;
        private Set<PartitionMapping> expectedMappings = ImmutableSet.of();

        public PartitionMappingTester withPartitionedSources(PlanNodeId... sources)
        {
            partitionedSources = ImmutableSet.copyOf(sources);
            return this;
        }

        public PartitionMappingTester withSplitPartitionCount(int splitPartitionCount)
        {
            this.splitPartitionCount = splitPartitionCount;
            return this;
        }

        public PartitionMappingTester withPartitionToNodeMap(Optional<List<InternalNode>> partitionToNodeMap)
        {
            this.partitionToNodeMap = partitionToNodeMap;
            return this;
        }

        public PartitionMappingTester withTargetPartitionSizeInBytes(long targetPartitionSizeInBytes)
        {
            this.targetPartitionSizeInBytes = targetPartitionSizeInBytes;
            return this;
        }

        public PartitionMappingTester withTargetMinTaskCount(int targetMinTaskCount)
        {
            this.targetMinTaskCount = targetMinTaskCount;
            return this;
        }

        public PartitionMappingTester withSourceDataSizeEstimates(Map<PlanNodeId, OutputDataSizeEstimate> sourceDataSizeEstimates)
        {
            this.sourceDataSizeEstimates = sourceDataSizeEstimates;
            return this;
        }

        public PartitionMappingTester withSplittableSources(PlanNodeId... sources)
        {
            splittableSources = ImmutableSet.copyOf(sources);
            return this;
        }

        public PartitionMappingTester withMergeAllowed(boolean mergeAllowed)
        {
            this.mergeAllowed = mergeAllowed;
            return this;
        }

        public PartitionMappingTester withExpectedMappings(PartitionMapping... mappings)
        {
            expectedMappings = ImmutableSet.copyOf(mappings);
            return this;
        }

        public void run()
        {
            FaultTolerantPartitioningScheme partitioningScheme = createPartitioningScheme(splitPartitionCount, partitionToNodeMap);
            Map<Integer, TaskPartition> actual = createSourcePartitionToTaskPartition(
                    partitioningScheme,
                    partitionedSources,
                    sourceDataSizeEstimates,
                    targetPartitionSizeInBytes,
                    targetMinTaskCount,
                    Integer.MAX_VALUE,
                    splittableSources::contains,
                    mergeAllowed);
            Set<PartitionMapping> actualGroups = extractMappings(actual);
            assertThat(actualGroups).isEqualTo(expectedMappings);
        }

        private static Set<PartitionMapping> extractMappings(Map<Integer, TaskPartition> sourcePartitionToTaskPartition)
        {
            SetMultimap<TaskPartition, Integer> grouped = sourcePartitionToTaskPartition.entrySet().stream()
                    .collect(toImmutableSetMultimap(Map.Entry::getValue, Map.Entry::getKey));
            return Multimaps.asMap(grouped).entrySet().stream()
                    .map(entry -> new PartitionMapping(entry.getValue(), entry.getKey().getSubPartitions().size()))
                    .collect(toImmutableSet());
        }
    }

    private record PartitionMapping(Set<Integer> sourcePartitions, int taskPartitionCount)
    {
        private PartitionMapping
        {
            sourcePartitions = ImmutableSet.copyOf(requireNonNull(sourcePartitions, "sourcePartitions is null"));
        }
    }
}
