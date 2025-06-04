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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSingleDistributionSplitAssigner
{
    private static final PlanNodeId PLAN_NODE_1 = new PlanNodeId("plan-node-1");
    private static final PlanNodeId PLAN_NODE_2 = new PlanNodeId("plan-node-2");

    @Test
    public void testNoSources()
    {
        Optional<HostAddress> hostRequirement = Optional.of(HostAddress.fromParts("localhost", 8080));
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(hostRequirement, ImmutableSet.of());
        SplitAssignerTester tester = new SplitAssignerTester();

        tester.update(splitAssigner.finish());

        assertThat(tester.getTaskPartitionCount()).isEqualTo(1);
        assertThat(tester.getNodeRequirements(0)).isEqualTo(new NodeRequirements(Optional.empty(), hostRequirement, false));
        assertThat(tester.isSealed(0)).isTrue();
        assertThat(tester.isNoMoreTaskPartitions()).isTrue();
    }

    @Test
    public void testEmptySource()
    {
        Optional<HostAddress> hostRequirement = Optional.of(HostAddress.fromParts("localhost", 8080));
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(
                hostRequirement,
                ImmutableSet.of(PLAN_NODE_1));
        SplitAssignerTester tester = new SplitAssignerTester();

        tester.update(splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(), true));
        tester.update(splitAssigner.finish());

        assertThat(tester.getTaskPartitionCount()).isEqualTo(1);
        assertThat(tester.getNodeRequirements(0)).isEqualTo(new NodeRequirements(Optional.empty(), hostRequirement, false));
        assertThat(tester.getSplitIds(0, PLAN_NODE_1)).isEmpty();
        assertThat(tester.isNoMoreSplits(0, PLAN_NODE_1)).isTrue();
        assertThat(tester.isSealed(0)).isTrue();
        assertThat(tester.isNoMoreTaskPartitions()).isTrue();
    }

    @Test
    public void testSingleSource()
    {
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(
                Optional.empty(),
                ImmutableSet.of(PLAN_NODE_1));
        SplitAssignerTester tester = new SplitAssignerTester();

        assertThat(tester.getTaskPartitionCount()).isEqualTo(0);
        assertThat(tester.isNoMoreTaskPartitions()).isFalse();

        tester.update(splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(1)), false));
        tester.update(splitAssigner.finish());
        assertThat(tester.getTaskPartitionCount()).isEqualTo(1);
        assertThat(tester.getSplitIds(0, PLAN_NODE_1)).containsExactly(1);
        assertThat(tester.isNoMoreTaskPartitions()).isTrue();

        tester.update(splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(2), 1, createSplit(3)), false));
        tester.update(splitAssigner.finish());
        assertThat(tester.getTaskPartitionCount()).isEqualTo(1);
        assertThat(tester.getSplitIds(0, PLAN_NODE_1)).containsExactly(1, 2, 3);

        assertThat(tester.isNoMoreSplits(0, PLAN_NODE_1)).isFalse();
        assertThat(tester.isSealed(0)).isFalse();
        tester.update(splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(4)), true));
        tester.update(splitAssigner.finish());
        assertThat(tester.isNoMoreSplits(0, PLAN_NODE_1)).isTrue();
        assertThat(tester.isSealed(0)).isTrue();
    }

    @Test
    public void testMultipleSources()
    {
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(
                Optional.empty(),
                ImmutableSet.of(PLAN_NODE_1, PLAN_NODE_2));
        SplitAssignerTester tester = new SplitAssignerTester();

        assertThat(tester.getTaskPartitionCount()).isEqualTo(0);
        assertThat(tester.isNoMoreTaskPartitions()).isFalse();

        tester.update(splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(1)), false));
        tester.update(splitAssigner.finish());
        assertThat(tester.getTaskPartitionCount()).isEqualTo(1);
        assertThat(tester.getSplitIds(0, PLAN_NODE_1)).containsExactlyInAnyOrder(1);
        assertThat(tester.isNoMoreTaskPartitions()).isTrue();

        tester.update(splitAssigner.assign(PLAN_NODE_2, ImmutableListMultimap.of(0, createSplit(2), 1, createSplit(3)), false));
        tester.update(splitAssigner.finish());
        assertThat(tester.getTaskPartitionCount()).isEqualTo(1);
        assertThat(tester.getSplitIds(0, PLAN_NODE_2)).containsExactlyInAnyOrder(2, 3);

        assertThat(tester.isNoMoreSplits(0, PLAN_NODE_1)).isFalse();
        tester.update(splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(2, createSplit(4)), true));
        tester.update(splitAssigner.finish());
        assertThat(tester.getSplitIds(0, PLAN_NODE_1)).containsExactlyInAnyOrder(1, 4);
        assertThat(tester.isNoMoreSplits(0, PLAN_NODE_1)).isTrue();

        assertThat(tester.isNoMoreSplits(0, PLAN_NODE_2)).isFalse();
        assertThat(tester.isSealed(0)).isFalse();
        tester.update(splitAssigner.assign(PLAN_NODE_2, ImmutableListMultimap.of(3, createSplit(5)), true));
        tester.update(splitAssigner.finish());
        assertThat(tester.getSplitIds(0, PLAN_NODE_2)).containsExactlyInAnyOrder(2, 3, 5);
        assertThat(tester.isNoMoreSplits(0, PLAN_NODE_2)).isTrue();
        assertThat(tester.isSealed(0)).isTrue();
    }

    private Split createSplit(int id)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.empty(), Optional.empty()));
    }
}
