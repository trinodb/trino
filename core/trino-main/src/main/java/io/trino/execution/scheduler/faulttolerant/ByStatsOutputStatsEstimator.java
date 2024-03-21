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
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.execution.StageId;
import io.trino.execution.scheduler.OutputDataSizeEstimate;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.LongStream;

import static io.trino.SystemSessionProperties.isFaultTolerantExecutionStageEstimationByStatsEnabled;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public class ByStatsOutputStatsEstimator
        implements OutputStatsEstimator
{
    public static class Factory
            implements OutputStatsEstimatorFactory
    {
        @Override
        public OutputStatsEstimator create(Session session)
        {
            return new ByStatsOutputStatsEstimator(isFaultTolerantExecutionStageEstimationByStatsEnabled(session));
        }
    }

    private final boolean enabled;

    public ByStatsOutputStatsEstimator(boolean enabled)
    {
        this.enabled = enabled;
    }

    @Override
    public Optional<OutputStatsEstimateResult> getEstimatedOutputStats(
            EventDrivenFaultTolerantQueryScheduler.StageExecution stageExecution,
            Function<StageId, EventDrivenFaultTolerantQueryScheduler.StageExecution> stageExecutionLookup,
            boolean parentEager)
    {
        if (!enabled) {
            return Optional.empty();
        }
        PlanNodeStatsEstimate stats = stageExecution.getStats();
        double outputRowCount = stats.getOutputRowCount();
        double size = stats.getOutputSizeInBytes(stats.getSymbolsWithKnownStatistics());
        if (!isFinite(outputRowCount) || isNaN(size)) {
            return Optional.empty();
        }
        int partitionsCount = stageExecution.getPartitionsCount();
        // Assume uniform output-size distribution across all partitions.
        ImmutableLongArray sizes = ImmutableLongArray.copyOf(LongStream.generate(() -> (long) (size / partitionsCount)).limit(partitionsCount));
        return Optional.of(new OutputStatsEstimateResult(new OutputDataSizeEstimate(sizes), (long) outputRowCount, "BY_STATS", false));
    }
}
