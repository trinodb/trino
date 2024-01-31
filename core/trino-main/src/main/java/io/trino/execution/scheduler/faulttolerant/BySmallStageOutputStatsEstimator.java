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

import com.google.common.primitives.ImmutableLongArray;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.StageId;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.StageExecution;
import io.trino.spi.QueryId;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.SystemSessionProperties.getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionSmallStageEstimationThreshold;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionSmallStageSourceSizeMultiplier;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionSmallStageEstimationEnabled;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionSmallStageRequireNoMorePartitions;
import static java.util.Objects.requireNonNull;

public class BySmallStageOutputStatsEstimator
        implements OutputStatsEstimator
{
    public static class Factory
            implements OutputStatsEstimatorFactory
    {
        @Override
        public OutputStatsEstimator create(Session session)
        {
            return new BySmallStageOutputStatsEstimator(
                    session.getQueryId(),
                    isFaultTolerantExecutionSmallStageEstimationEnabled(session),
                    getFaultTolerantExecutionSmallStageEstimationThreshold(session),
                    getFaultTolerantExecutionSmallStageSourceSizeMultiplier(session),
                    getFaultTolerantExecutionArbitraryDistributionComputeTaskTargetSizeMin(session),
                    isFaultTolerantExecutionSmallStageRequireNoMorePartitions(session));
        }
    }

    private final QueryId queryId;
    private final boolean smallStageEstimationEnabled;
    private final DataSize smallStageEstimationThreshold;
    private final double smallStageSourceSizeMultiplier;
    private final DataSize smallSizePartitionSizeEstimate;
    private final boolean smallStageRequireNoMorePartitions;

    private BySmallStageOutputStatsEstimator(
            QueryId queryId,
            boolean smallStageEstimationEnabled,
            DataSize smallStageEstimationThreshold,
            double smallStageSourceSizeMultiplier,
            DataSize smallSizePartitionSizeEstimate,
            boolean smallStageRequireNoMorePartitions)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.smallStageEstimationEnabled = smallStageEstimationEnabled;
        this.smallStageEstimationThreshold = requireNonNull(smallStageEstimationThreshold, "smallStageEstimationThreshold is null");
        this.smallStageSourceSizeMultiplier = smallStageSourceSizeMultiplier;
        this.smallSizePartitionSizeEstimate = requireNonNull(smallSizePartitionSizeEstimate, "smallSizePartitionSizeEstimate is null");
        this.smallStageRequireNoMorePartitions = smallStageRequireNoMorePartitions;
    }

    @Override
    public Optional<OutputStatsEstimateResult> getEstimatedOutputStats(StageExecution stageExecution, Function<StageId, StageExecution> stageExecutionLookup, boolean parentEager)
    {
        if (!smallStageEstimationEnabled) {
            return Optional.empty();
        }

        if (smallStageRequireNoMorePartitions && !stageExecution.isNoMorePartitions()) {
            return Optional.empty();
        }

        long[] currentOutputDataSize = stageExecution.currentOutputDataSize();
        long totaleOutputDataSize = 0;
        for (long partitionOutputDataSize : currentOutputDataSize) {
            totaleOutputDataSize += partitionOutputDataSize;
        }
        if (totaleOutputDataSize > smallStageEstimationThreshold.toBytes()) {
            // our output is too big already
            return Optional.empty();
        }

        PlanFragment planFragment = stageExecution.getStageInfo().getPlan();
        boolean hasPartitionedSources = planFragment.getPartitionedSources().size() > 0;
        List<RemoteSourceNode> remoteSourceNodes = planFragment.getRemoteSourceNodes();

        long partitionedInputSizeEstimate = 0;
        if (hasPartitionedSources) {
            if (!stageExecution.isNoMorePartitions()) {
                // stage is reading directly from table
                // for leaf stages require all tasks to be enumerated
                return Optional.empty();
            }
            // estimate partitioned input based on number of task partitions
            partitionedInputSizeEstimate += stageExecution.getPartitionsCount() * smallSizePartitionSizeEstimate.toBytes();
        }

        long remoteInputSizeEstimate = 0;
        for (RemoteSourceNode remoteSourceNode : remoteSourceNodes) {
            for (PlanFragmentId sourceFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                StageId sourceStageId = StageId.create(queryId, sourceFragmentId);

                StageExecution sourceStage = stageExecutionLookup.apply(sourceStageId);
                requireNonNull(sourceStage, "sourceStage is null");
                Optional<OutputStatsEstimateResult> sourceStageOutputDataSize = sourceStage.getOutputStats(stageExecutionLookup, false);

                if (sourceStageOutputDataSize.isEmpty()) {
                    // cant estimate size of one of sources; should not happen in practice
                    return Optional.empty();
                }

                remoteInputSizeEstimate += sourceStageOutputDataSize.orElseThrow().outputDataSizeEstimate().getTotalSizeInBytes();
            }
        }

        long inputSizeEstimate = (long) ((partitionedInputSizeEstimate + remoteInputSizeEstimate) * smallStageSourceSizeMultiplier);
        if (inputSizeEstimate > smallStageEstimationThreshold.toBytes()) {
            return Optional.empty();
        }

        int outputPartitionsCount = stageExecution.getSinkPartitioningScheme().getPartitionCount();
        ImmutableLongArray.Builder estimateBuilder = ImmutableLongArray.builder(outputPartitionsCount);
        for (int i = 0; i < outputPartitionsCount; ++i) {
            // assume uniform distribution
            // TODO; should we use distribution as in this.outputDataSize if we have some data there already?
            estimateBuilder.add(inputSizeEstimate / outputPartitionsCount);
        }
        // TODO: For now we can skip calculating outputRowCountEstimate since we won't run adaptive planner in the case of small inputs
        return Optional.of(new OutputStatsEstimateResult(estimateBuilder.build(), 0, "BY_SMALL_INPUT"));
    }
}
