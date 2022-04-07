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

import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.spi.ErrorCode;

import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTaskMemoryGrowthFactor;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;

public class ExponentialGrowthPartitionMemoryEstimator
        implements PartitionMemoryEstimator
{
    @Override
    public MemoryRequirements getInitialMemoryRequirements(Session session, DataSize defaultMemoryLimit)
    {
        return new MemoryRequirements(
                defaultMemoryLimit,
                false);
    }

    @Override
    public MemoryRequirements getNextRetryMemoryRequirements(Session session, MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, ErrorCode errorCode)
    {
        DataSize previousMemory = previousMemoryRequirements.getRequiredMemory();
        DataSize baseMemory = Ordering.natural().max(peakMemoryUsage, previousMemory);
        if (shouldIncreaseMemory(errorCode)) {
            double growthFactor = getFaultTolerantExecutionTaskMemoryGrowthFactor(session);
            return new MemoryRequirements(DataSize.of((long) (baseMemory.toBytes() * growthFactor), DataSize.Unit.BYTE), false);
        }
        return new MemoryRequirements(baseMemory, false);
    }

    private boolean shouldIncreaseMemory(ErrorCode errorCode)
    {
        return EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode().equals(errorCode) // too many tasks from single query on a node
                || CLUSTER_OUT_OF_MEMORY.toErrorCode().equals(errorCode); // too many tasks in general on a node
    }
}
