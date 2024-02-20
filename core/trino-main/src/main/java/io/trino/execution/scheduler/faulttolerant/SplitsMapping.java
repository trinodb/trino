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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.INTEGER_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public final class SplitsMapping
{
    private static final int INSTANCE_SIZE = instanceSize(SplitsMapping.class);

    public static final SplitsMapping EMPTY = SplitsMapping.builder().build();

    // not using Multimap to avoid extensive data structure copying when building updated SplitsMapping
    private final Map<PlanNodeId, Map<Integer, List<Split>>> splits; // plan-node -> hash-partition -> Split

    private SplitsMapping(ImmutableMap<PlanNodeId, Map<Integer, List<Split>>> splits)
    {
        // Builder implementations ensure that external map as well as Maps/Lists used in values
        // are immutable.
        this.splits = splits;
    }

    public Set<PlanNodeId> getPlanNodeIds()
    {
        return splits.keySet();
    }

    public ListMultimap<PlanNodeId, Split> getSplitsFlat()
    {
        ImmutableListMultimap.Builder<PlanNodeId, Split> splitsFlat = ImmutableListMultimap.builder();
        for (Map.Entry<PlanNodeId, Map<Integer, List<Split>>> entry : splits.entrySet()) {
            // TODO can we do less copying?
            splitsFlat.putAll(entry.getKey(), entry.getValue().values().stream().flatMap(Collection::stream).collect(toImmutableList()));
        }
        return splitsFlat.build();
    }

    public List<Split> getSplitsFlat(PlanNodeId planNodeId)
    {
        Map<Integer, List<Split>> splits = this.splits.get(planNodeId);
        if (splits == null) {
            return ImmutableList.of();
        }
        verify(!splits.isEmpty(), "expected not empty splits list %s", splits);

        if (splits.size() == 1) {
            return getOnlyElement(splits.values());
        }

        // TODO improve to not copy here; return view instead
        ImmutableList.Builder<Split> result = ImmutableList.builder();
        for (List<Split> partitionSplits : splits.values()) {
            result.addAll(partitionSplits);
        }
        return result.build();
    }

    @VisibleForTesting
    ListMultimap<Integer, Split> getSplits(PlanNodeId planNodeId)
    {
        Map<Integer, List<Split>> splits = this.splits.get(planNodeId);
        if (splits == null) {
            return ImmutableListMultimap.of();
        }
        verify(!splits.isEmpty(), "expected not empty splits list %s", splits);

        ImmutableListMultimap.Builder<Integer, Split> result = ImmutableListMultimap.builder();
        for (Map.Entry<Integer, List<Split>> entry : splits.entrySet()) {
            result.putAll(entry.getKey(), entry.getValue());
        }
        return result.build();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                estimatedSizeOf(
                        splits,
                        PlanNodeId::getRetainedSizeInBytes,
                        planNodeSplits -> estimatedSizeOf(
                                planNodeSplits,
                                partitionId -> INTEGER_INSTANCE_SIZE,
                                splitList -> estimatedSizeOf(splitList, Split::getRetainedSizeInBytes)));
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
        SplitsMapping that = (SplitsMapping) o;
        return Objects.equals(splits, that.splits);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(splits);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splits", splits)
                .toString();
    }

    public static Builder builder()
    {
        return new NewBuilder();
    }

    public static Builder builder(SplitsMapping mapping)
    {
        return new UpdatingBuilder(mapping);
    }

    public long size()
    {
        return splits.values().stream()
                .flatMap(sourcePartitionToSplits -> sourcePartitionToSplits.values().stream())
                .mapToLong(List::size)
                .sum();
    }

    public abstract static class Builder
    {
        private Builder() {} // close for extension

        public Builder addSplit(PlanNodeId planNodeId, int partitionId, Split split)
        {
            return addSplits(planNodeId, partitionId, ImmutableList.of(split));
        }

        public Builder addSplits(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits)
        {
            Multimaps.asMap(splits).forEach((partitionId, partitionSplits) -> addSplits(planNodeId, partitionId, partitionSplits));
            return this;
        }

        public Builder addMapping(SplitsMapping updatingMapping)
        {
            for (Map.Entry<PlanNodeId, Map<Integer, List<Split>>> entry : updatingMapping.splits.entrySet()) {
                PlanNodeId planNodeId = entry.getKey();
                entry.getValue().forEach((partitionId, partitionSplits) -> addSplits(planNodeId, partitionId, partitionSplits));
            }
            return this;
        }

        public abstract Builder addSplits(PlanNodeId planNodeId, int partitionId, List<Split> splits);

        public abstract SplitsMapping build();
    }

    private static class UpdatingBuilder
            extends Builder
    {
        private final SplitsMapping originalMapping;
        private final Map<PlanNodeId, Map<Integer, ImmutableList.Builder<Split>>> updates = new HashMap<>();

        public UpdatingBuilder(SplitsMapping originalMapping)
        {
            this.originalMapping = requireNonNull(originalMapping, "sourceMapping is null");
        }

        @Override
        public Builder addSplits(PlanNodeId planNodeId, int partitionId, List<Split> splits)
        {
            if (splits.isEmpty()) {
                // ensure we do not have empty lists in result splits map.
                return this;
            }
            updates.computeIfAbsent(planNodeId, ignored -> new HashMap<>())
                    .computeIfAbsent(partitionId, key -> ImmutableList.builder())
                    .addAll(splits);
            return this;
        }

        @Override
        public SplitsMapping build()
        {
            ImmutableMap.Builder<PlanNodeId, Map<Integer, List<Split>>> result = ImmutableMap.builder();
            for (PlanNodeId planNodeId : Sets.union(originalMapping.splits.keySet(), updates.keySet())) {
                Map<Integer, List<Split>> planNodeOriginalMapping = originalMapping.splits.getOrDefault(planNodeId, ImmutableMap.of());
                Map<Integer, ImmutableList.Builder<Split>> planNodeUpdates = updates.getOrDefault(planNodeId, ImmutableMap.of());
                if (planNodeUpdates.isEmpty()) {
                    // just use original splits for planNodeId
                    result.put(planNodeId, planNodeOriginalMapping);
                    continue;
                }
                // create new mapping for planNodeId reusing as much of source as possible
                ImmutableMap.Builder<Integer, List<Split>> targetSplitsMapBuilder = ImmutableMap.builder();
                for (Integer sourcePartitionId : Sets.union(planNodeOriginalMapping.keySet(), planNodeUpdates.keySet())) {
                    @Nullable List<Split> originalSplits = planNodeOriginalMapping.get(sourcePartitionId);
                    @Nullable ImmutableList.Builder<Split> splitUpdates = planNodeUpdates.get(sourcePartitionId);
                    targetSplitsMapBuilder.put(sourcePartitionId, mergeIfPresent(originalSplits, splitUpdates));
                }
                result.put(planNodeId, targetSplitsMapBuilder.buildOrThrow());
            }
            return new SplitsMapping(result.buildOrThrow());
        }

        private static <T> List<T> mergeIfPresent(@Nullable List<T> list, @Nullable ImmutableList.Builder<T> additionalElements)
        {
            if (additionalElements == null) {
                // reuse source immutable split list
                return requireNonNull(list, "list is null");
            }
            if (list == null) {
                return additionalElements.build();
            }
            return ImmutableList.<T>builder()
                    .addAll(list)
                    .addAll(additionalElements.build())
                    .build();
        }
    }

    private static class NewBuilder
            extends Builder
    {
        private final Map<PlanNodeId, Map<Integer, ImmutableList.Builder<Split>>> splitsBuilder = new HashMap<>();

        @Override
        public Builder addSplits(PlanNodeId planNodeId, int partitionId, List<Split> splits)
        {
            if (splits.isEmpty()) {
                // ensure we do not have empty lists in result splits map.
                return this;
            }
            splitsBuilder.computeIfAbsent(planNodeId, ignored -> new HashMap<>())
                    .computeIfAbsent(partitionId, ignored -> ImmutableList.builder())
                    .addAll(splits);
            return this;
        }

        @Override
        public SplitsMapping build()
        {
            return new SplitsMapping(splitsBuilder.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            planNodeMapping -> planNodeMapping.getValue().entrySet().stream()
                                    .collect(toImmutableMap(
                                            Map.Entry::getKey,
                                            sourcePartitionMapping -> sourcePartitionMapping.getValue().build())))));
        }
    }
}
