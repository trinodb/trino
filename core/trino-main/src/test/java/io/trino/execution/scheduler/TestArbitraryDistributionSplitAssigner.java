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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestArbitraryDistributionSplitAssigner
{
    private static final int FUZZ_TESTING_INVOCATION_COUNT = 100;

    private static final long STANDARD_SPLIT_SIZE_IN_BYTES = 1;

    private static final PlanNodeId PARTITIONED_1 = new PlanNodeId("partitioned-1");
    private static final PlanNodeId PARTITIONED_2 = new PlanNodeId("partitioned-2");
    private static final PlanNodeId REPLICATED_1 = new PlanNodeId("replicated-1");
    private static final PlanNodeId REPLICATED_2 = new PlanNodeId("replicated-2");

    private static final HostAddress HOST_1 = HostAddress.fromParts("localhost", 8081);
    private static final HostAddress HOST_2 = HostAddress.fromParts("localhost", 8082);
    private static final HostAddress HOST_3 = HostAddress.fromParts("localhost", 8083);

    @Test
    public void testEmpty()
    {
        // single partitioned source
        SplitAssigner splitAssigner = createSplitAssigner(ImmutableSet.of(PARTITIONED_1), ImmutableSet.of(), 100, false);
        SplitAssignerTester tester = new SplitAssignerTester();
        tester.update(splitAssigner.assign(PARTITIONED_1, ImmutableListMultimap.of(), true));
        assertTrue(tester.isNoMoreSplits(0, PARTITIONED_1));
        tester.update(splitAssigner.finish());
        List<TaskDescriptor> taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        assertThat(taskDescriptors).hasSize(1);
        assertTaskDescriptor(taskDescriptors.get(0), 0, ImmutableListMultimap.of());

        // single replicated source
        splitAssigner = createSplitAssigner(ImmutableSet.of(), ImmutableSet.of(REPLICATED_1), 100, false);
        tester = new SplitAssignerTester();
        tester.update(splitAssigner.assign(REPLICATED_1, ImmutableListMultimap.of(), true));
        assertTrue(tester.isNoMoreSplits(0, REPLICATED_1));
        tester.update(splitAssigner.finish());
        taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        assertThat(taskDescriptors).hasSize(1);
        assertTaskDescriptor(taskDescriptors.get(0), 0, ImmutableListMultimap.of());

        // partitioned and replicates source
        splitAssigner = createSplitAssigner(ImmutableSet.of(PARTITIONED_1), ImmutableSet.of(REPLICATED_1), 100, true);
        tester = new SplitAssignerTester();
        tester.update(splitAssigner.assign(REPLICATED_1, ImmutableListMultimap.of(), true));
        assertFalse(tester.isNoMoreSplits(0, PARTITIONED_1));
        assertFalse(tester.isNoMoreSplits(0, REPLICATED_1));
        tester.update(splitAssigner.assign(PARTITIONED_1, ImmutableListMultimap.of(), true));
        assertTrue(tester.isNoMoreSplits(0, PARTITIONED_1));
        assertTrue(tester.isNoMoreSplits(0, REPLICATED_1));
        tester.update(splitAssigner.finish());
        taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        assertThat(taskDescriptors).hasSize(1);
        assertTaskDescriptor(taskDescriptors.get(0), 0, ImmutableListMultimap.of());

        splitAssigner = createSplitAssigner(ImmutableSet.of(PARTITIONED_1), ImmutableSet.of(REPLICATED_1), 100, true);
        tester = new SplitAssignerTester();
        tester.update(splitAssigner.assign(PARTITIONED_1, ImmutableListMultimap.of(), true));
        assertFalse(tester.isNoMoreSplits(0, PARTITIONED_1));
        assertFalse(tester.isNoMoreSplits(0, REPLICATED_1));
        tester.update(splitAssigner.assign(REPLICATED_1, ImmutableListMultimap.of(), true));
        assertTrue(tester.isNoMoreSplits(0, PARTITIONED_1));
        assertTrue(tester.isNoMoreSplits(0, REPLICATED_1));
        tester.update(splitAssigner.finish());
        taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        assertThat(taskDescriptors).hasSize(1);
        assertTaskDescriptor(taskDescriptors.get(0), 0, ImmutableListMultimap.of());

        splitAssigner = createSplitAssigner(ImmutableSet.of(PARTITIONED_1, PARTITIONED_2), ImmutableSet.of(REPLICATED_1, REPLICATED_2), 100, true);
        tester = new SplitAssignerTester();
        tester.update(splitAssigner.assign(REPLICATED_1, ImmutableListMultimap.of(), true));
        tester.update(splitAssigner.assign(PARTITIONED_1, ImmutableListMultimap.of(), true));
        tester.update(splitAssigner.assign(PARTITIONED_2, ImmutableListMultimap.of(), true));
        assertFalse(tester.isNoMoreSplits(0, PARTITIONED_1));
        assertFalse(tester.isNoMoreSplits(0, REPLICATED_1));
        assertFalse(tester.isNoMoreSplits(0, PARTITIONED_2));
        assertFalse(tester.isNoMoreSplits(0, REPLICATED_2));
        tester.update(splitAssigner.assign(REPLICATED_2, ImmutableListMultimap.of(), true));
        assertTrue(tester.isNoMoreSplits(0, PARTITIONED_1));
        assertTrue(tester.isNoMoreSplits(0, REPLICATED_1));
        assertTrue(tester.isNoMoreSplits(0, PARTITIONED_2));
        assertTrue(tester.isNoMoreSplits(0, REPLICATED_2));

        tester.update(splitAssigner.finish());
        taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        assertThat(taskDescriptors).hasSize(1);
        assertTaskDescriptor(taskDescriptors.get(0), 0, ImmutableListMultimap.of());
    }

    @Test
    public void testNoHostRequirement()
    {
        // no splits
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(new SplitBatch(PARTITIONED_1, ImmutableList.of(), true)),
                1,
                false);

        // single partitioned source
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), true)),
                // one split per partition
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1), createSplit(2)), true)),
                1,
                false);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1), createSplit(2)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(3), createSplit(4)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(5)), true)),
                // two splits per partition
                2,
                true);

        // multiple partitioned sources
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(2)), true)),
                1,
                false);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(2), createSplit(3)), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(4)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(5)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(6)), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(1)), true),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(2), createSplit(3)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(4)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(5)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(6)), true)),
                2,
                false);

        // single replicated source
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(3)), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(1)), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(3)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(4)), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(5)), true)),
                1,
                false);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(2)), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(3)), true)),
                2,
                false);

        // multiple replicates sources
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), true),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(3)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(4)), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(3)), true),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(4)), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1)), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2), createSplit(3), createSplit(4), createSplit(5)), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(6), createSplit(7)), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(8), createSplit(9)), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(10), createSplit(11), createSplit(12), createSplit(13)), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(14), createSplit(15)), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(16), createSplit(17), createSplit(18), createSplit(19)), true),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(20)), false),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(21), createSplit(22)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(23)), true),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(24), createSplit(25)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(26), createSplit(27)), true)),
                3,
                true);
    }

    @Test
    public void testWithHostRequirement()
    {
        // single partitioned source
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1))), true)),
                // one split per partition
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1, HOST_2))), true)),
                // one split per partition
                1,
                false);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1, HOST_2)), createSplit(2, ImmutableList.of(HOST_2))), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1, HOST_2))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(2, ImmutableList.of(HOST_1))), true)),
                // two splits per partition
                2,
                false);

        testAssigner(
                ImmutableSet.of(PARTITIONED_1),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1, HOST_2)), createSplit(2, ImmutableList.of(HOST_1, HOST_2))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(3, ImmutableList.of(HOST_3)), createSplit(4, ImmutableList.of(HOST_1))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(5)), true)),
                // two splits per partition
                2,
                true);

        // multiple partitioned sources
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_3))), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(2, ImmutableList.of(HOST_3))), true)),
                1,
                false);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_3))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(2, ImmutableList.of(HOST_3)), createSplit(3, ImmutableList.of(HOST_2))), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(4, ImmutableList.of(HOST_1))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(5)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(6, ImmutableList.of(HOST_3))), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1, HOST_2))), true),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(2), createSplit(3, ImmutableList.of(HOST_3))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(4, ImmutableList.of(HOST_1, HOST_2))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(5, ImmutableList.of(HOST_3))), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(6, ImmutableList.of(HOST_1, HOST_2))), true)),
                2,
                false);

        // single replicated source
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_3))), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(3, ImmutableList.of(HOST_2))), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(1)), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(3, ImmutableList.of(HOST_3))), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(4, ImmutableList.of(HOST_3))), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(5)), true)),
                1,
                false);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1))), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(2, ImmutableList.of(HOST_2))), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(3)), true)),
                2,
                true);

        // multiple replicates sources
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_1))), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), true),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(3)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(4, ImmutableList.of(HOST_1))), true)),
                1,
                false);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_2))), true),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(3, ImmutableList.of(HOST_2))), true),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(4)), true)),
                1,
                true);
        testAssigner(
                ImmutableSet.of(PARTITIONED_1, PARTITIONED_2),
                ImmutableSet.of(REPLICATED_1, REPLICATED_2),
                ImmutableList.of(
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1, ImmutableList.of(HOST_2))), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(2), createSplit(3), createSplit(4), createSplit(5)), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(6, ImmutableList.of(HOST_2, HOST_3)), createSplit(7)), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(8), createSplit(9, ImmutableList.of(HOST_2, HOST_3))), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(10), createSplit(11), createSplit(12), createSplit(13)), false),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(14), createSplit(15, ImmutableList.of(HOST_1, HOST_3))), false),
                        new SplitBatch(REPLICATED_1, ImmutableList.of(createSplit(16), createSplit(17), createSplit(18), createSplit(19)), true),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(20, ImmutableList.of(HOST_1, HOST_3))), false),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(21), createSplit(22)), false),
                        new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(23, ImmutableList.of(HOST_2, HOST_3))), true),
                        new SplitBatch(REPLICATED_2, ImmutableList.of(createSplit(24), createSplit(25)), true),
                        new SplitBatch(PARTITIONED_2, ImmutableList.of(createSplit(26), createSplit(27, ImmutableList.of(HOST_1, HOST_3))), true)),
                3,
                false);
    }

    @Test
    public void fuzzTestingNoHostRequirement()
    {
        for (int i = 0; i < FUZZ_TESTING_INVOCATION_COUNT; i++) {
            fuzzTesting(false);
        }
    }

    @Test
    public void fuzzTestingWithHostRequirement()
    {
        for (int i = 0; i < FUZZ_TESTING_INVOCATION_COUNT; i++) {
            fuzzTesting(true);
        }
    }

    @Test
    public void testAdaptiveTaskSizing()
    {
        Set<PlanNodeId> partitionedSources = ImmutableSet.of(PARTITIONED_1);
        List<SplitBatch> batches = ImmutableList.of(
                new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1), createSplit(2), createSplit(3)), false),
                new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(4), createSplit(5), createSplit(6)), false),
                new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(7), createSplit(8), createSplit(9)), true));
        SplitAssigner splitAssigner = new ArbitraryDistributionSplitAssigner(
                Optional.of(TEST_CATALOG_HANDLE),
                partitionedSources,
                ImmutableSet.of(),
                1,
                1.2,
                1,
                4,
                STANDARD_SPLIT_SIZE_IN_BYTES,
                5);
        SplitAssignerTester tester = new SplitAssignerTester();
        for (SplitBatch batch : batches) {
            PlanNodeId planNodeId = batch.getPlanNodeId();
            List<Split> splits = batch.getSplits();
            boolean noMoreSplits = batch.isNoMoreSplits();
            tester.update(splitAssigner.assign(planNodeId, createSplitsMultimap(splits), noMoreSplits));
            tester.checkContainsSplits(planNodeId, splits, false);
        }
        tester.update(splitAssigner.finish());
        List<TaskDescriptor> taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        assertThat(taskDescriptors).hasSize(4);

        TaskDescriptor taskDescriptor0 = taskDescriptors.get(0);
        assertTaskDescriptor(
                taskDescriptor0,
                taskDescriptor0.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(1))
                        .build());
        TaskDescriptor taskDescriptor1 = taskDescriptors.get(1);
        assertTaskDescriptor(
                taskDescriptor1,
                taskDescriptor1.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(2))
                        .put(PARTITIONED_1, createSplit(3))
                        .build());
        TaskDescriptor taskDescriptor2 = taskDescriptors.get(2);
        assertTaskDescriptor(
                taskDescriptor2,
                taskDescriptor2.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(4))
                        .put(PARTITIONED_1, createSplit(5))
                        .put(PARTITIONED_1, createSplit(6))
                        .build());
        TaskDescriptor taskDescriptor3 = taskDescriptors.get(3);
        assertTaskDescriptor(
                taskDescriptor3,
                taskDescriptor3.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(7))
                        .put(PARTITIONED_1, createSplit(8))
                        .put(PARTITIONED_1, createSplit(9))
                        .build());
    }

    @Test
    public void testAdaptiveTaskSizingRounding()
    {
        Set<PlanNodeId> partitionedSources = ImmutableSet.of(PARTITIONED_1);
        List<SplitBatch> batches = ImmutableList.of(
                new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(1), createSplit(2), createSplit(3)), false),
                new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(4), createSplit(5), createSplit(6)), false),
                new SplitBatch(PARTITIONED_1, ImmutableList.of(createSplit(7), createSplit(8), createSplit(9)), true));
        SplitAssigner splitAssigner = new ArbitraryDistributionSplitAssigner(
                Optional.of(TEST_CATALOG_HANDLE),
                partitionedSources,
                ImmutableSet.of(),
                1,
                1.3,
                100,
                400,
                100,
                5);
        SplitAssignerTester tester = new SplitAssignerTester();
        for (SplitBatch batch : batches) {
            PlanNodeId planNodeId = batch.getPlanNodeId();
            List<Split> splits = batch.getSplits();
            boolean noMoreSplits = batch.isNoMoreSplits();
            tester.update(splitAssigner.assign(planNodeId, createSplitsMultimap(splits), noMoreSplits));
            tester.checkContainsSplits(planNodeId, splits, false);
        }
        tester.update(splitAssigner.finish());
        List<TaskDescriptor> taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        assertThat(taskDescriptors).hasSize(5);

        // target size 100, round to 100
        TaskDescriptor taskDescriptor0 = taskDescriptors.get(0);
        assertTaskDescriptor(
                taskDescriptor0,
                taskDescriptor0.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(1))
                        .build());

        // target size 130, round to 100
        TaskDescriptor taskDescriptor1 = taskDescriptors.get(1);
        assertTaskDescriptor(
                taskDescriptor1,
                taskDescriptor1.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(2))
                        .build());

        // target size 169, round to 200
        TaskDescriptor taskDescriptor2 = taskDescriptors.get(2);
        assertTaskDescriptor(
                taskDescriptor2,
                taskDescriptor2.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(3))
                        .put(PARTITIONED_1, createSplit(4))
                        .build());

        // target size 220, round to 200
        TaskDescriptor taskDescriptor3 = taskDescriptors.get(3);
        assertTaskDescriptor(
                taskDescriptor3,
                taskDescriptor3.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(5))
                        .put(PARTITIONED_1, createSplit(6))
                        .build());

        // target size 286, round to 300
        TaskDescriptor taskDescriptor4 = taskDescriptors.get(4);
        assertTaskDescriptor(
                taskDescriptor4,
                taskDescriptor4.getPartitionId(),
                ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .put(PARTITIONED_1, createSplit(7))
                        .put(PARTITIONED_1, createSplit(8))
                        .put(PARTITIONED_1, createSplit(9))
                        .build());
    }

    private void fuzzTesting(boolean withHostRequirements)
    {
        Set<PlanNodeId> partitionedSources = new HashSet<>();
        Set<PlanNodeId> replicatedSources = new HashSet<>();
        partitionedSources.add(PARTITIONED_1);
        if (ThreadLocalRandom.current().nextBoolean()) {
            partitionedSources.add(PARTITIONED_2);
        }
        if (ThreadLocalRandom.current().nextDouble() > 0.2) {
            replicatedSources.add(REPLICATED_1);
        }
        if (ThreadLocalRandom.current().nextDouble() > 0.5) {
            replicatedSources.add(REPLICATED_2);
        }
        Set<PlanNodeId> allSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedSources)
                .addAll(replicatedSources)
                .build();

        List<SplitBatch> batches = new ArrayList<>();
        Map<PlanNodeId, Integer> splitCount = allSources.stream()
                .collect(Collectors.toMap(Function.identity(), planNodeId -> ThreadLocalRandom.current().nextInt(100)));

        AtomicInteger nextSplitId = new AtomicInteger();
        while (!splitCount.isEmpty()) {
            List<PlanNodeId> remainingSources = ImmutableList.copyOf(splitCount.keySet());
            PlanNodeId source = remainingSources.get(ThreadLocalRandom.current().nextInt(remainingSources.size()));
            int batchSize = ThreadLocalRandom.current().nextInt(5);
            int remaining = splitCount.compute(source, (key, value) -> value - batchSize);
            if (remaining <= 0) {
                splitCount.remove(source);
            }
            List<Split> splits = IntStream.range(0, batchSize)
                    .mapToObj(value -> generateSplit(nextSplitId, replicatedSources.contains(source), withHostRequirements))
                    .collect(toImmutableList());
            batches.add(new SplitBatch(source, splits, remaining <= 0));
        }

        int splitsPerPartition = ThreadLocalRandom.current().nextInt(3);
        testAssigner(partitionedSources, replicatedSources, batches, splitsPerPartition, ThreadLocalRandom.current().nextBoolean());
    }

    private Split generateSplit(AtomicInteger nextSplitId, boolean replicated, boolean withHostRequirements)
    {
        if (replicated || !withHostRequirements || ThreadLocalRandom.current().nextDouble() > 0.5) {
            return createSplit(nextSplitId.getAndIncrement());
        }
        List<HostAddress> allHosts = new ArrayList<>();
        allHosts.add(HOST_1);
        allHosts.add(HOST_2);
        allHosts.add(HOST_3);
        shuffle(allHosts);
        List<HostAddress> addresses = ImmutableList.copyOf(allHosts.subList(0, ThreadLocalRandom.current().nextInt(1, allHosts.size())));
        return createSplit(nextSplitId.getAndIncrement(), addresses);
    }

    private static void testAssigner(
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            List<SplitBatch> batches,
            int partitionedSplitsPerPartition,
            boolean verifyMaxTaskSplitCount)
    {
        SplitAssigner splitAssigner = createSplitAssigner(partitionedSources, replicatedSources, partitionedSplitsPerPartition, verifyMaxTaskSplitCount);
        SplitAssignerTester tester = new SplitAssignerTester();
        ListMultimap<PlanNodeId, Split> expectedReplicatedSplits = ArrayListMultimap.create();
        Map<Integer, ListMultimap<PlanNodeId, Split>> expectedPartitionedSplits = new HashMap<>();
        Set<PlanNodeId> finishedReplicatedSources = new HashSet<>();
        Map<Optional<HostAddress>, PartitionAssignment> currentSplitAssignments = new HashMap<>();
        AtomicInteger nextPartitionId = new AtomicInteger();
        for (SplitBatch batch : batches) {
            PlanNodeId planNodeId = batch.getPlanNodeId();
            List<Split> splits = batch.getSplits();
            boolean noMoreSplits = batch.isNoMoreSplits();
            boolean replicated = replicatedSources.contains(planNodeId);
            if (replicated) {
                expectedReplicatedSplits.putAll(planNodeId, splits);
                if (noMoreSplits) {
                    finishedReplicatedSources.add(planNodeId);
                }
            }
            else {
                for (Split split : splits) {
                    Optional<HostAddress> hostRequirement = Optional.empty();
                    if (!split.isRemotelyAccessible()) {
                        int splitCount = Integer.MAX_VALUE;
                        for (HostAddress hostAddress : split.getConnectorSplit().getAddresses()) {
                            PartitionAssignment currentAssignment = currentSplitAssignments.get(Optional.of(hostAddress));
                            if (currentAssignment == null) {
                                hostRequirement = Optional.of(hostAddress);
                                break;
                            }
                            else if (currentAssignment.getSplits().size() < splitCount) {
                                splitCount = currentAssignment.getSplits().size();
                                hostRequirement = Optional.of(hostAddress);
                            }
                        }
                    }
                    PartitionAssignment currentAssignment = currentSplitAssignments.get(hostRequirement);
                    if (currentAssignment != null && currentAssignment.getSplits().size() + 1 > partitionedSplitsPerPartition) {
                        expectedPartitionedSplits.computeIfAbsent(currentAssignment.getPartitionId(), key -> ArrayListMultimap.create()).putAll(currentAssignment.getSplits());
                        currentSplitAssignments.remove(hostRequirement);
                    }
                    currentSplitAssignments
                            .computeIfAbsent(hostRequirement, key -> new PartitionAssignment(nextPartitionId.getAndIncrement()))
                            .getSplits()
                            .put(planNodeId, split);
                }
            }
            tester.update(splitAssigner.assign(planNodeId, createSplitsMultimap(splits), noMoreSplits));
            tester.checkContainsSplits(planNodeId, splits, replicated);

            if (finishedReplicatedSources.containsAll(replicatedSources)) {
                Set<Integer> openAssignments = currentSplitAssignments.values().stream()
                        .map(PartitionAssignment::getPartitionId)
                        .collect(toImmutableSet());
                for (int partitionId = 0; partitionId < nextPartitionId.get(); partitionId++) {
                    if (!openAssignments.contains(partitionId)) {
                        assertTrue(tester.isSealed(partitionId));
                    }
                }
            }
        }
        tester.update(splitAssigner.finish());
        for (PartitionAssignment assignment : currentSplitAssignments.values()) {
            expectedPartitionedSplits.computeIfAbsent(assignment.getPartitionId(), key -> ArrayListMultimap.create()).putAll(assignment.getSplits());
        }
        List<TaskDescriptor> taskDescriptors = tester.getTaskDescriptors().orElseThrow();
        int expectedPartitionCount = nextPartitionId.get();
        if (expectedPartitionCount == 0) {
            // a single partition is always created
            assertThat(taskDescriptors).hasSize(1);
            TaskDescriptor taskDescriptor = taskDescriptors.get(0);
            assertTaskDescriptor(
                    taskDescriptor,
                    taskDescriptor.getPartitionId(),
                    ImmutableListMultimap.copyOf(expectedReplicatedSplits));
        }
        else {
            assertThat(taskDescriptors).hasSize(expectedPartitionCount);
            for (TaskDescriptor taskDescriptor : taskDescriptors) {
                assertTaskDescriptor(
                        taskDescriptor,
                        taskDescriptor.getPartitionId(),
                        ImmutableListMultimap.<PlanNodeId, Split>builder()
                                .putAll(expectedReplicatedSplits)
                                .putAll(expectedPartitionedSplits.getOrDefault(taskDescriptor.getPartitionId(), ImmutableListMultimap.of()))
                                .build());
            }
        }
    }

    private static Split createSplit(int id)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.empty(), Optional.empty()));
    }

    private static Split createSplit(int id, List<HostAddress> addresses)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.empty(), Optional.of(addresses)));
    }

    private static ListMultimap<Integer, Split> createSplitsMultimap(List<Split> splits)
    {
        int nextPartitionId = 0;
        ImmutableListMultimap.Builder<Integer, Split> result = ImmutableListMultimap.builder();
        for (Split split : splits) {
            result.put(nextPartitionId++, split);
        }
        return result.build();
    }

    private static void assertTaskDescriptor(
            TaskDescriptor taskDescriptor,
            int expectedPartitionId,
            ListMultimap<PlanNodeId, Split> expectedSplits)
    {
        assertEquals(taskDescriptor.getPartitionId(), expectedPartitionId);
        assertSplitsEqual(taskDescriptor.getSplits(), expectedSplits);
        Set<HostAddress> hostRequirement = null;
        for (Split split : taskDescriptor.getSplits().values()) {
            if (!split.isRemotelyAccessible()) {
                if (hostRequirement == null) {
                    hostRequirement = ImmutableSet.copyOf(split.getAddresses());
                }
                else {
                    hostRequirement = Sets.intersection(hostRequirement, ImmutableSet.copyOf(split.getAddresses()));
                }
            }
        }
        assertEquals(taskDescriptor.getNodeRequirements().getCatalogHandle(), Optional.of(TEST_CATALOG_HANDLE));
        assertThat(taskDescriptor.getNodeRequirements().getAddresses()).containsAnyElementsOf(hostRequirement == null ? ImmutableSet.of() : hostRequirement);
    }

    private static void assertSplitsEqual(ListMultimap<PlanNodeId, Split> actual, ListMultimap<PlanNodeId, Split> expected)
    {
        SetMultimap<PlanNodeId, Integer> actualSplitIds = ImmutableSetMultimap.copyOf(Multimaps.transformValues(actual, TestingConnectorSplit::getSplitId));
        SetMultimap<PlanNodeId, Integer> expectedSplitIds = ImmutableSetMultimap.copyOf(Multimaps.transformValues(expected, TestingConnectorSplit::getSplitId));
        assertEquals(actualSplitIds, expectedSplitIds);
    }

    private static ArbitraryDistributionSplitAssigner createSplitAssigner(
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            int partitionedSplitsPerPartition,
            boolean verifyMaxTaskSplitCount)
    {
        long targetPartitionSizeInBytes = Long.MAX_VALUE;
        int maxTaskSplitCount = Integer.MAX_VALUE;
        // make sure both limits are tested
        if (verifyMaxTaskSplitCount) {
            maxTaskSplitCount = partitionedSplitsPerPartition;
        }
        else {
            targetPartitionSizeInBytes = STANDARD_SPLIT_SIZE_IN_BYTES * partitionedSplitsPerPartition;
        }
        return new ArbitraryDistributionSplitAssigner(
                Optional.of(TEST_CATALOG_HANDLE),
                partitionedSources,
                replicatedSources,
                Integer.MAX_VALUE,
                1.0,
                targetPartitionSizeInBytes,
                targetPartitionSizeInBytes,
                STANDARD_SPLIT_SIZE_IN_BYTES,
                maxTaskSplitCount);
    }

    private static class SplitBatch
    {
        private final PlanNodeId planNodeId;
        private final List<Split> splits;
        private final boolean noMoreSplits;

        public SplitBatch(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
            this.noMoreSplits = noMoreSplits;
        }

        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        public List<Split> getSplits()
        {
            return splits;
        }

        public boolean isNoMoreSplits()
        {
            return noMoreSplits;
        }
    }

    private static class PartitionAssignment
    {
        private final int partitionId;
        private final ListMultimap<PlanNodeId, Split> splits = ArrayListMultimap.create();

        private PartitionAssignment(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public ListMultimap<PlanNodeId, Split> getSplits()
        {
            return splits;
        }
    }
}
