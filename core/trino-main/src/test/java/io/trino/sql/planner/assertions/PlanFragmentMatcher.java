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
package io.trino.sql.planner.assertions;

import io.trino.Session;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static java.util.Objects.requireNonNull;

public class PlanFragmentMatcher
{
    private final PlanFragmentId fragmentId;
    private final Optional<PlanMatchPattern> planPattern;
    private final Optional<PartitioningHandle> partitioning;
    private final Optional<Integer> inputPartitionCount;
    private final Optional<Integer> outputPartitionCount;
    private final Optional<List<PlanNodeId>> partitionedSources;
    private final Optional<PartitioningScheme> outputPartitioningScheme;
    private final Optional<StatsAndCosts> statsAndCosts;

    public static Builder builder()
    {
        return new Builder();
    }

    public PlanFragmentMatcher(
            PlanFragmentId fragmentId,
            Optional<PlanMatchPattern> planPattern,
            Optional<PartitioningHandle> partitioning,
            Optional<Integer> inputPartitionCount,
            Optional<Integer> outputPartitionCount,
            Optional<List<PlanNodeId>> partitionedSources,
            Optional<PartitioningScheme> outputPartitioningScheme,
            Optional<StatsAndCosts> statsAndCosts)
    {
        this.fragmentId = requireNonNull(fragmentId, "fragmentId is null");
        this.planPattern = requireNonNull(planPattern, "planPattern is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.inputPartitionCount = requireNonNull(inputPartitionCount, "inputPartitionCount is null");
        this.outputPartitionCount = requireNonNull(outputPartitionCount, "outputPartitionCount is null");
        this.partitionedSources = requireNonNull(partitionedSources, "partitionedSources is null");
        this.outputPartitioningScheme = requireNonNull(outputPartitioningScheme, "outputPartitioningScheme is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
    }

    public boolean matches(PlanFragment fragment, StatsCalculator statsCalculator, Session session, Metadata metadata)
    {
        if (!fragmentId.equals(fragment.getId())) {
            return false;
        }
        if (planPattern.isPresent()) {
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, TypeProvider.viewOf(fragment.getSymbols()), new CachingTableStatsProvider(metadata, session));
            MatchResult matches = fragment.getRoot().accept(new PlanMatchingVisitor(session, metadata, statsProvider, noLookup()), planPattern.get());
            if (!matches.isMatch()) {
                return false;
            }
        }
        if (partitioning.isPresent() && !partitioning.get().equals(fragment.getPartitioning())) {
            return false;
        }
        if (inputPartitionCount.isPresent() && !inputPartitionCount.equals(fragment.getPartitionCount())) {
            return false;
        }
        if (outputPartitionCount.isPresent() && !outputPartitionCount.equals(fragment.getOutputPartitioningScheme().getPartitionCount())) {
            return false;
        }
        if (partitionedSources.isPresent() && !partitionedSources.get().equals(fragment.getPartitionedSources())) {
            return false;
        }
        if (outputPartitioningScheme.isPresent() && !outputPartitioningScheme.get().equals(fragment.getOutputPartitioningScheme())) {
            return false;
        }
        return statsAndCosts.isEmpty() || statsAndCosts.get().equals(fragment.getStatsAndCosts());
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Fragment ").append(fragmentId).append("\n");
        planPattern.ifPresent(planPattern -> builder.append("PlanPattern: \n").append(planPattern).append("\n"));
        partitioning.ifPresent(partitioning -> builder.append("Partitioning: ").append(partitioning).append("\n"));
        outputPartitioningScheme.ifPresent(outputPartitioningScheme -> builder.append("OutputPartitioningScheme: ").append(outputPartitioningScheme).append("\n"));
        inputPartitionCount.ifPresent(partitionCount -> builder.append("InputPartitionCount: ").append(partitionCount).append("\n"));
        outputPartitionCount.ifPresent(partitionCount -> builder.append("OutputPartitionCount: ").append(partitionCount).append("\n"));
        partitionedSources.ifPresent(partitionedSources -> builder.append("PartitionedSources: ").append(partitionedSources).append("\n"));
        statsAndCosts.ifPresent(statsAndCosts -> builder.append("StatsAndCosts: ").append(statsAndCosts).append("\n"));
        return builder.toString();
    }

    public static class Builder
    {
        private PlanFragmentId fragmentId;
        private Optional<PlanMatchPattern> planPattern = Optional.empty();
        private Optional<PartitioningHandle> partitioning = Optional.empty();
        private Optional<Integer> inputPartitionCount = Optional.empty();
        private Optional<Integer> outputPartitionCount = Optional.empty();
        private Optional<List<PlanNodeId>> partitionedSources = Optional.empty();
        private Optional<PartitioningScheme> outputPartitioningScheme = Optional.empty();
        private Optional<StatsAndCosts> statsAndCosts = Optional.empty();

        public Builder fragmentId(int fragmentId)
        {
            this.fragmentId = new PlanFragmentId(String.valueOf(fragmentId));
            return this;
        }

        public Builder planPattern(PlanMatchPattern planPattern)
        {
            this.planPattern = Optional.of(planPattern);
            return this;
        }

        public Builder partitioning(PartitioningHandle partitioning)
        {
            this.partitioning = Optional.of(partitioning);
            return this;
        }

        public Builder inputPartitionCount(int inputPartitionCount)
        {
            this.inputPartitionCount = Optional.of(inputPartitionCount);
            return this;
        }

        public Builder outputPartitionCount(int outputPartitionCount)
        {
            this.outputPartitionCount = Optional.of(outputPartitionCount);
            return this;
        }

        public Builder partitionedSources(List<PlanNodeId> partitionedSources)
        {
            this.partitionedSources = Optional.of(partitionedSources);
            return this;
        }

        public Builder outputPartitioningScheme(PartitioningScheme outputPartitioningScheme)
        {
            this.outputPartitioningScheme = Optional.of(outputPartitioningScheme);
            return this;
        }

        public Builder statsAndCosts(StatsAndCosts statsAndCosts)
        {
            this.statsAndCosts = Optional.of(statsAndCosts);
            return this;
        }

        public PlanFragmentMatcher build()
        {
            return new PlanFragmentMatcher(
                    fragmentId,
                    planPattern,
                    partitioning,
                    inputPartitionCount,
                    outputPartitionCount,
                    partitionedSources,
                    outputPartitioningScheme,
                    statsAndCosts);
        }
    }
}
