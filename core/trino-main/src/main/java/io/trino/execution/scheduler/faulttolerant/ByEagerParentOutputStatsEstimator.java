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
import io.trino.execution.StageId;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.StageExecution;

import java.util.Optional;
import java.util.function.Function;

public class ByEagerParentOutputStatsEstimator
        implements OutputStatsEstimator
{
    public static class Factory
            implements OutputStatsEstimatorFactory
    {
        @Override
        public OutputStatsEstimator create(Session session)
        {
            return new ByEagerParentOutputStatsEstimator();
        }
    }

    @Override
    public Optional<OutputStatsEstimateResult> getEstimatedOutputStats(StageExecution stageExecution, Function<StageId, StageExecution> stageExecutionLookup, boolean parentEager)
    {
        if (!parentEager) {
            return Optional.empty();
        }

        // use empty estimate as fallback for eager parents. It matches current logic of assessing if node should be processed eagerly or not.
        // Currently, we use eager task exectuion only for stages with small FINAL LIMIT which implies small input from child stages (child stages will
        // enforce small input via PARTIAL LIMIT)
        int outputPartitionsCount = stageExecution.getSinkPartitioningScheme().getPartitionCount();
        ImmutableLongArray.Builder estimateBuilder = ImmutableLongArray.builder(outputPartitionsCount);
        for (int i = 0; i < outputPartitionsCount; ++i) {
            estimateBuilder.add(0);
        }
        return Optional.of(new OutputStatsEstimateResult(estimateBuilder.build(), 0, "FOR_EAGER_PARENT"));
    }
}
