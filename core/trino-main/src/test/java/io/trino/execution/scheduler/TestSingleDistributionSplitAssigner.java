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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSingleDistributionSplitAssigner
{
    private static final PlanNodeId PLAN_NODE_1 = new PlanNodeId("plan-node-1");
    private static final PlanNodeId PLAN_NODE_2 = new PlanNodeId("plan-node-2");

    @Test
    public void testNoSources()
    {
        ImmutableSet<HostAddress> hostRequirement = ImmutableSet.of(HostAddress.fromParts("localhost", 8080));
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(hostRequirement, ImmutableSet.of());
        TestingTaskSourceCallback callback = new TestingTaskSourceCallback();

        splitAssigner.finish().update(callback);

        assertEquals(callback.getPartitionCount(), 1);
        assertEquals(callback.getNodeRequirements(0), new NodeRequirements(Optional.empty(), hostRequirement));
        assertTrue(callback.isSealed(0));
        assertTrue(callback.isNoMorePartitions());
    }

    @Test
    public void testEmptySource()
    {
        ImmutableSet<HostAddress> hostRequirement = ImmutableSet.of(HostAddress.fromParts("localhost", 8080));
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(
                hostRequirement,
                ImmutableSet.of(PLAN_NODE_1));
        TestingTaskSourceCallback callback = new TestingTaskSourceCallback();

        splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(), true).update(callback);
        splitAssigner.finish().update(callback);

        assertEquals(callback.getPartitionCount(), 1);
        assertEquals(callback.getNodeRequirements(0), new NodeRequirements(Optional.empty(), hostRequirement));
        assertThat(callback.getSplitIds(0, PLAN_NODE_1)).isEmpty();
        assertTrue(callback.isNoMoreSplits(0, PLAN_NODE_1));
        assertTrue(callback.isSealed(0));
        assertTrue(callback.isNoMorePartitions());
    }

    @Test
    public void testSingleSource()
    {
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(
                ImmutableSet.of(),
                ImmutableSet.of(PLAN_NODE_1));
        TestingTaskSourceCallback callback = new TestingTaskSourceCallback();

        assertEquals(callback.getPartitionCount(), 0);
        assertFalse(callback.isNoMorePartitions());

        splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(1)), false).update(callback);
        splitAssigner.finish().update(callback);
        assertEquals(callback.getPartitionCount(), 1);
        assertThat(callback.getSplitIds(0, PLAN_NODE_1)).containsExactly(1);
        assertTrue(callback.isNoMorePartitions());

        splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(2), 1, createSplit(3)), false).update(callback);
        splitAssigner.finish().update(callback);
        assertEquals(callback.getPartitionCount(), 1);
        assertThat(callback.getSplitIds(0, PLAN_NODE_1)).containsExactly(1, 2, 3);

        assertFalse(callback.isNoMoreSplits(0, PLAN_NODE_1));
        assertFalse(callback.isSealed(0));
        splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(4)), true).update(callback);
        splitAssigner.finish().update(callback);
        assertTrue(callback.isNoMoreSplits(0, PLAN_NODE_1));
        assertTrue(callback.isSealed(0));
    }

    @Test
    public void testMultipleSources()
    {
        SplitAssigner splitAssigner = new SingleDistributionSplitAssigner(
                ImmutableSet.of(),
                ImmutableSet.of(PLAN_NODE_1, PLAN_NODE_2));
        TestingTaskSourceCallback callback = new TestingTaskSourceCallback();

        assertEquals(callback.getPartitionCount(), 0);
        assertFalse(callback.isNoMorePartitions());

        splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(0, createSplit(1)), false).update(callback);
        splitAssigner.finish().update(callback);
        assertEquals(callback.getPartitionCount(), 1);
        assertThat(callback.getSplitIds(0, PLAN_NODE_1)).containsExactly(1);
        assertTrue(callback.isNoMorePartitions());

        splitAssigner.assign(PLAN_NODE_2, ImmutableListMultimap.of(0, createSplit(2), 1, createSplit(3)), false).update(callback);
        splitAssigner.finish().update(callback);
        assertEquals(callback.getPartitionCount(), 1);
        assertThat(callback.getSplitIds(0, PLAN_NODE_2)).containsExactly(2, 3);

        assertFalse(callback.isNoMoreSplits(0, PLAN_NODE_1));
        splitAssigner.assign(PLAN_NODE_1, ImmutableListMultimap.of(2, createSplit(4)), true).update(callback);
        splitAssigner.finish().update(callback);
        assertThat(callback.getSplitIds(0, PLAN_NODE_1)).containsExactly(1, 4);
        assertTrue(callback.isNoMoreSplits(0, PLAN_NODE_1));

        assertFalse(callback.isNoMoreSplits(0, PLAN_NODE_2));
        assertFalse(callback.isSealed(0));
        splitAssigner.assign(PLAN_NODE_2, ImmutableListMultimap.of(3, createSplit(5)), true).update(callback);
        splitAssigner.finish().update(callback);
        assertThat(callback.getSplitIds(0, PLAN_NODE_2)).containsExactly(2, 3, 5);
        assertTrue(callback.isNoMoreSplits(0, PLAN_NODE_2));
        assertTrue(callback.isSealed(0));
    }

    private Split createSplit(int id)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.empty(), Optional.empty()));
    }
}
