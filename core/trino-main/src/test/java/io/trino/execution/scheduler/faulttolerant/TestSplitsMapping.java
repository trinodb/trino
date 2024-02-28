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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.guava.api.Assertions.assertThat;

public class TestSplitsMapping
{
    @Test
    public void testNewSplitMappingBuilder()
    {
        SplitsMapping.Builder newBuilder = SplitsMapping.builder();
        newBuilder.addSplit(new PlanNodeId("N1"), 0, createSplit(1));
        newBuilder.addSplit(new PlanNodeId("N1"), 1, createSplit(2));
        newBuilder.addSplits(new PlanNodeId("N1"), 1, ImmutableList.of(createSplit(3), createSplit(4)));
        newBuilder.addSplits(new PlanNodeId("N1"), 2, ImmutableList.of(createSplit(5), createSplit(6))); // addSplits(list) creating new source partition
        newBuilder.addSplits(new PlanNodeId("N1"), ImmutableListMultimap.of(
                0, createSplit(7),
                1, createSplit(8),
                3, createSplit(9))); // create new source partition
        newBuilder.addSplit(new PlanNodeId("N2"), 0, createSplit(10)); // another plan node
        newBuilder.addSplit(new PlanNodeId("N2"), 3, createSplit(11));
        newBuilder.addMapping(SplitsMapping.builder()
                .addSplit(new PlanNodeId("N1"), 0, createSplit(20))
                .addSplit(new PlanNodeId("N1"), 4, createSplit(21))
                .addSplit(new PlanNodeId("N3"), 0, createSplit(22))
                .build());

        SplitsMapping splitsMapping1 = newBuilder.build();

        assertThat(splitsMapping1.getPlanNodeIds()).containsExactlyInAnyOrder(new PlanNodeId("N1"), new PlanNodeId("N2"), new PlanNodeId("N3"));
        assertThat(splitIds(splitsMapping1, "N1")).isEqualTo(
                ImmutableListMultimap.builder()
                        .putAll(0, 1, 7, 20)
                        .putAll(1, 2, 3, 4, 8)
                        .putAll(2, 5, 6)
                        .putAll(3, 9)
                        .put(4, 21)
                        .build());
        assertThat(splitIds(splitsMapping1, "N2")).isEqualTo(
                ImmutableListMultimap.builder()
                        .put(0, 10)
                        .put(3, 11)
                        .build());
        assertThat(splitIds(splitsMapping1, "N3")).isEqualTo(
                ImmutableListMultimap.builder()
                        .put(0, 22)
                        .build());
    }

    @Test
    public void testUpdatingSplitMappingBuilder()
    {
        SplitsMapping.Builder newBuilder = SplitsMapping.builder(SplitsMapping.builder()
                .addSplit(new PlanNodeId("N1"), 0, createSplit(20))
                .addSplit(new PlanNodeId("N1"), 4, createSplit(21))
                .addSplit(new PlanNodeId("N3"), 0, createSplit(22))
                .build());

        newBuilder.addSplit(new PlanNodeId("N1"), 0, createSplit(1));
        newBuilder.addSplit(new PlanNodeId("N1"), 1, createSplit(2));
        newBuilder.addSplits(new PlanNodeId("N1"), 1, ImmutableList.of(createSplit(3), createSplit(4)));
        newBuilder.addSplits(new PlanNodeId("N1"), 2, ImmutableList.of(createSplit(5), createSplit(6))); // addSplits(list) creating new source partition
        newBuilder.addSplits(new PlanNodeId("N1"), ImmutableListMultimap.of(
                0, createSplit(7),
                1, createSplit(8),
                3, createSplit(9))); // create new source partition
        newBuilder.addSplit(new PlanNodeId("N2"), 0, createSplit(10)); // another plan node
        newBuilder.addSplit(new PlanNodeId("N2"), 3, createSplit(11));

        SplitsMapping splitsMapping1 = newBuilder.build();

        assertThat(splitsMapping1.getPlanNodeIds()).containsExactlyInAnyOrder(new PlanNodeId("N1"), new PlanNodeId("N2"), new PlanNodeId("N3"));
        assertThat(splitIds(splitsMapping1, "N1")).isEqualTo(
                ImmutableListMultimap.builder()
                        .putAll(0, 20, 1, 7)
                        .putAll(1, 2, 3, 4, 8)
                        .putAll(2, 5, 6)
                        .putAll(3, 9)
                        .put(4, 21)
                        .build());
        assertThat(splitIds(splitsMapping1, "N2")).isEqualTo(
                ImmutableListMultimap.builder()
                        .put(0, 10)
                        .put(3, 11)
                        .build());
        assertThat(splitIds(splitsMapping1, "N3")).isEqualTo(
                ImmutableListMultimap.builder()
                        .put(0, 22)
                        .build());
    }

    private ListMultimap<Integer, Integer> splitIds(SplitsMapping splitsMapping, String planNodeId)
    {
        return splitsMapping.getSplits(new PlanNodeId(planNodeId)).entries().stream()
                .collect(ImmutableListMultimap.toImmutableListMultimap(
                        Map.Entry::getKey,
                        entry -> ((TestingConnectorSplit) entry.getValue().getConnectorSplit()).getId()));
    }

    private static Split createSplit(int id)
    {
        return new Split(TEST_CATALOG_HANDLE, new TestingConnectorSplit(id, OptionalInt.empty(), Optional.empty()));
    }
}
