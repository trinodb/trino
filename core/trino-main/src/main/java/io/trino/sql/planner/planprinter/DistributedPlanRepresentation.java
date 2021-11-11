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
package io.trino.sql.planner.planprinter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.StageInfo;
import io.trino.execution.StageStats;
import io.trino.operator.StageExecutionDescriptor;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.planprinter.DistributedFragmentRepresentation.getFragmentRepresentation;
import static io.trino.sql.planner.planprinter.PlanPrinter.formatHash;
import static java.util.Objects.requireNonNull;

public class DistributedPlanRepresentation
{
    private final List<DistributedPlan> distributedPlans;

    @JsonCreator
    public DistributedPlanRepresentation(@JsonProperty("plan") List<DistributedPlan> distributedPlans)
    {
        this.distributedPlans = requireNonNull(distributedPlans, "distributedPlans is null");
    }

    @JsonProperty
    public List<DistributedPlan> getDistributedPlans()
    {
        return distributedPlans;
    }

    @Immutable
    public static class DistributedPlan
    {
        private final PlanFragmentId id;
        private final Optional<DistributedPlanStats> distributedPlanStats;
        private final PartitioningHandle partitioning;
        private final String outputLayout;
        private final OutputPartitioning outputPartitioning;
        private final StageExecutionDescriptor.StageExecutionStrategy stageExecutionStrategy;
        private final Map<Integer, List<DistributedFragmentRepresentation>> distributedFragmentRepresentation;
        private final PlanRepresentation planRepresentation;

        @JsonCreator
        public DistributedPlan(
                @JsonProperty("id") PlanFragmentId id,
                @JsonProperty("partitioning") PartitioningHandle partitioning,
                @JsonProperty("planStats") Optional<DistributedPlanStats> distributedPlanStats,
                @JsonProperty("outputLayout") String outputLayout,
                @JsonProperty("outputPartitioning") OutputPartitioning outputPartitioning,
                @JsonProperty("stageExecutionStrategy") StageExecutionDescriptor.StageExecutionStrategy stageExecutionStrategy,
                @JsonProperty("distributedFragmentPlan") Map<Integer, List<DistributedFragmentRepresentation>> distributedFragmentRepresentation,
                @JsonProperty("planRepresentation") PlanRepresentation planRepresentation)
        {
            this.id = requireNonNull(id, "id is null");
            this.partitioning = requireNonNull(partitioning, "partitioning is null");
            this.distributedPlanStats = distributedPlanStats;
            this.outputLayout = requireNonNull(outputLayout, "outputLayout is null");
            this.outputPartitioning = requireNonNull(outputPartitioning, "outputPartitioning is null");
            this.stageExecutionStrategy = requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null");
            this.distributedFragmentRepresentation = requireNonNull(distributedFragmentRepresentation, "distributedPlanRepresentation is null");
            this.planRepresentation = requireNonNull(planRepresentation, "planRepresentation is null");
        }

        @JsonProperty
        public PlanFragmentId getId()
        {
            return id;
        }

        @JsonProperty
        public PartitioningHandle getPartitioning()
        {
            return partitioning;
        }

        @JsonProperty
        public Optional<DistributedPlanStats> getDistributedPlanStats()
        {
            return distributedPlanStats;
        }

        @JsonProperty
        public String getOutputLayout()
        {
            return outputLayout;
        }

        @JsonProperty
        public OutputPartitioning getOutputPartitioning()
        {
            return outputPartitioning;
        }

        @JsonProperty
        public StageExecutionDescriptor.StageExecutionStrategy getStageExecutionStrategy()
        {
            return stageExecutionStrategy;
        }

        @JsonProperty
        public Map<Integer, List<DistributedFragmentRepresentation>> getDistributedFragmentRepresentation()
        {
            return distributedFragmentRepresentation;
        }

        @JsonIgnore
        public PlanRepresentation getPlanRepresentation()
        {
            return planRepresentation;
        }

        public static class OutputPartitioning
        {
            private final PartitioningHandle handle;
            private final String arguments;
            private final String hashColumn;

            @JsonCreator
            public OutputPartitioning(
                    @JsonProperty("handle") PartitioningHandle handle,
                    @JsonProperty("arguments") String arguments,
                    @JsonProperty("hashColumn") String hashColumn)
            {
                this.handle = handle;
                this.arguments = arguments;
                this.hashColumn = hashColumn;
            }

            @JsonProperty
            public PartitioningHandle getHandle()
            {
                return handle;
            }

            @JsonProperty
            public String getArguments()
            {
                return arguments;
            }

            @JsonProperty
            public String getHashColumn()
            {
                return hashColumn;
            }
        }

        @Immutable
        public static class DistributedPlanStats
        {
            private final Duration totalCpuTime;
            private final Duration totalScheduledTime;
            private final long inputRows;
            private final DataSize inputSize;
            private final Double averageInputRows;
            private final Double stdDevInputRows;
            private final long outputRows;
            private final DataSize outputSize;

            @JsonCreator
            public DistributedPlanStats(
                    @JsonProperty("totalCpuTime") Duration totalCpuTime,
                    @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
                    @JsonProperty("inputRows") long inputRows,
                    @JsonProperty("inputSize") DataSize inputSize,
                    @JsonProperty("averageInputRows") Double averageInputRows,
                    @JsonProperty("stdDevInputRows") Double stdDevInputRows,
                    @JsonProperty("outputRows") long outputRows,
                    @JsonProperty("outputSize") DataSize outputSize)
            {
                this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
                this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
                this.inputRows = inputRows;
                this.inputSize = requireNonNull(inputSize, "inputSize is null");
                this.averageInputRows = requireNonNull(averageInputRows, "averageInputRows is null");
                this.stdDevInputRows = requireNonNull(stdDevInputRows, "stdDevInputRows is null");
                this.outputRows = outputRows;
                this.outputSize = requireNonNull(outputSize, "outputSize is null");
            }

            @JsonProperty
            public Duration getTotalCpuTime()
            {
                return totalCpuTime;
            }

            @JsonProperty
            public Duration getTotalScheduledTime()
            {
                return totalScheduledTime;
            }

            @JsonProperty
            public long getInputRows()
            {
                return inputRows;
            }

            @JsonProperty
            public DataSize getInputSize()
            {
                return inputSize;
            }

            @JsonProperty
            public Double getAverageInputRows()
            {
                return averageInputRows;
            }

            @JsonProperty
            public Double getStdDevInputRows()
            {
                return stdDevInputRows;
            }

            @JsonProperty
            public long getOutputRows()
            {
                return outputRows;
            }

            @JsonProperty
            public DataSize getOutputSize()
            {
                return outputSize;
            }
        }
    }

    public static DistributedPlan getDistributedPlan(
            PlanRepresentation planRepresentation,
            ValuePrinter valuePrinter,
            PlanFragment planFragment,
            Optional<StageInfo> stageInfo,
            boolean verbose)
    {
        StageStats stageStats;
        Optional<DistributedPlan.DistributedPlanStats> distributedPlanStats = Optional.empty();
        if (stageInfo.isPresent()) {
            stageStats = stageInfo.get().getStageStats();

            double avgPositionsPerTask = stageInfo.get().getTasks().stream().mapToLong(task -> task.getStats().getProcessedInputPositions()).average().orElse(Double.NaN);
            double squaredDifferences = stageInfo.get().getTasks().stream().mapToDouble(task -> Math.pow(task.getStats().getProcessedInputPositions() - avgPositionsPerTask, 2)).sum();
            double sdAmongTasks = Math.sqrt(squaredDifferences / stageInfo.get().getTasks().size());

            distributedPlanStats = Optional.of(new DistributedPlan.DistributedPlanStats(
                    stageStats.getTotalCpuTime(),
                    stageStats.getTotalScheduledTime(),
                    stageStats.getProcessedInputPositions(),
                    stageStats.getProcessedInputDataSize(),
                    avgPositionsPerTask,
                    sdAmongTasks,
                    stageStats.getOutputPositions(),
                    stageStats.getOutputDataSize()));
        }

        PartitioningScheme partitioningScheme = planFragment.getPartitioningScheme();
        String outputLayout = Joiner.on(", ").join(partitioningScheme.getOutputLayout());

        List<String> arguments = getPartitioningSchemeArguments(valuePrinter, partitioningScheme);
        DistributedPlan.OutputPartitioning outputPartitioning = new DistributedPlan.OutputPartitioning(
                partitioningScheme.getPartitioning().getHandle(),
                Joiner.on(", ").join(arguments),
                formatHash(partitioningScheme.getHashColumn()));

        Map<Integer, List<DistributedFragmentRepresentation>> distributedFragmentRepresentations = getDistributedFragmentRepresentations(planRepresentation, verbose, 1);

        return new DistributedPlan(planFragment.getId(),
                planFragment.getPartitioning(),
                distributedPlanStats,
                outputLayout,
                outputPartitioning,
                planFragment.getStageExecutionDescriptor().getStageExecutionStrategy(),
                distributedFragmentRepresentations,
                planRepresentation);
    }

    private static Map<Integer, List<DistributedFragmentRepresentation>> getDistributedFragmentRepresentations(PlanRepresentation plan, boolean verbose, int level)
    {
        NodeRepresentation node = plan.getRoot();
        Map<Integer, List<DistributedFragmentRepresentation>> distributedFragmentRepresentationMap = new HashMap<>();
        distributedFragmentRepresentationMap.computeIfAbsent(level, i -> new ArrayList<>()).add(getFragmentRepresentation(plan, node, verbose, level));
        return distributedFragmentRepresentationMap;
    }

    public static List<String> getPartitioningSchemeArguments(ValuePrinter valuePrinter, PartitioningScheme partitioningScheme)
    {
        return partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        NullableValue constant = argument.getConstant();
                        String printableValue = valuePrinter.castToVarchar(constant.getType(), constant.getValue());
                        return constant.getType().getDisplayName() + "(" + printableValue + ")";
                    }
                    return argument.getColumn().toString();
                })
                .collect(toImmutableList());
    }
}
