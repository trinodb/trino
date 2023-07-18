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
import io.trino.execution.StageId;
import io.trino.execution.scheduler.OutputDataSizeEstimate;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public interface OutputStatsEstimator
{
    Optional<OutputStatsEstimateResult> getEstimatedOutputStats(
            EventDrivenFaultTolerantQueryScheduler.StageExecution stageExecution,
            Function<StageId, EventDrivenFaultTolerantQueryScheduler.StageExecution> stageExecutionLookup,
            boolean parentEager);

    record OutputStatsEstimateResult(
            OutputDataSizeEstimate outputDataSizeEstimate,
            long outputRowCountEstimate,
            String kind)
    {
        OutputStatsEstimateResult(ImmutableLongArray partitionDataSizes, long outputRowCountEstimate, String kind)
        {
            this(new OutputDataSizeEstimate(partitionDataSizes), outputRowCountEstimate, kind);
        }

        public OutputStatsEstimateResult
        {
            requireNonNull(outputDataSizeEstimate, "outputDataSizeEstimate is null");
            requireNonNull(kind, "kind is null");
        }
    }
}
