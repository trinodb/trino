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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;
import io.trino.operator.StageExecutionDescriptor;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.plan.PlanFragmentId;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class JsonDistributedFragment
{
    private final PlanFragmentId id;
    private final DistributedFragmentStats distributedFragmentStats;
    private final PartitioningHandle partitioning;
    private final String outputLayout;
    private final OutputPartitioning outputPartitioning;
    private final StageExecutionDescriptor.StageExecutionStrategy stageExecutionStrategy;
    private final Map<Integer, List<DistributedPlanRepresentation>> distributedPlanRepresentation;

    @JsonCreator
    public JsonDistributedFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("partitioning") PartitioningHandle partitioning,
            @JsonProperty("fragmentStats") DistributedFragmentStats distributedFragmentStats,
            @JsonProperty("outputLayout") String outputLayout,
            @JsonProperty("outputPartitioning") OutputPartitioning outputPartitioning,
            @JsonProperty("stageExecutionStrategy") StageExecutionDescriptor.StageExecutionStrategy stageExecutionStrategy,
            @JsonProperty("distributedPlan") Map<Integer, List<DistributedPlanRepresentation>> distributedPlanRepresentation)
    {
        this.id = requireNonNull(id, "id is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.distributedFragmentStats = requireNonNull(distributedFragmentStats, "distributedFragmentStats is null");
        this.outputLayout = requireNonNull(outputLayout, "outputLayout is null");
        this.outputPartitioning = requireNonNull(outputPartitioning, "outputPartitioning is null");
        this.stageExecutionStrategy = requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null");
        this.distributedPlanRepresentation = requireNonNull(distributedPlanRepresentation, "distributedPlanRepresentation is null");
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
    public DistributedFragmentStats getDistributedFragmentStats()
    {
        return distributedFragmentStats;
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
    public Map<Integer, List<DistributedPlanRepresentation>> getDistributedPlanRepresentation()
    {
        return distributedPlanRepresentation;
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
    public static class DistributedFragmentStats
    {
        private final Duration totalCpuTime;
        private final Duration totalScheduledTime;
        private final String inputRows;
        private final String inputDataSize;
        private final String averageInputRows;
        private final String stdDevInputRows;
        private final String outputRows;
        private final String outputDataSize;

        @JsonCreator
        public DistributedFragmentStats(
                @JsonProperty("totalCpuTime") Duration totalCpuTime,
                @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
                @JsonProperty("inputRows") String inputRows,
                @JsonProperty("inputDataSize") String inputDataSize,
                @JsonProperty("averageInputRows") String averageInputRows,
                @JsonProperty("stdDevInputRows") String stdDevInputRows,
                @JsonProperty("outputRows") String outputRows,
                @JsonProperty("outputDataSize") String outputDataSize)
        {
            this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
            this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
            this.inputRows = requireNonNull(inputRows, "inputRows is null");
            this.inputDataSize = requireNonNull(inputDataSize, "inputDataSize is null");
            this.averageInputRows = requireNonNull(averageInputRows, "averageInputRows is null");
            this.stdDevInputRows = requireNonNull(stdDevInputRows, "stdDevInputRows is null");
            this.outputRows = requireNonNull(outputRows, "outputRows is null");
            this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
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
        public String getInputRows()
        {
            return inputRows;
        }

        @JsonProperty
        public String getInputDataSize()
        {
            return inputDataSize;
        }

        @JsonProperty
        public String getAverageInputRows()
        {
            return averageInputRows;
        }

        @JsonProperty
        public String getStdDevInputRows()
        {
            return stdDevInputRows;
        }

        @JsonProperty
        public String getOutputRows()
        {
            return outputRows;
        }

        @JsonProperty
        public String getOutputDataSize()
        {
            return outputDataSize;
        }
    }
}
