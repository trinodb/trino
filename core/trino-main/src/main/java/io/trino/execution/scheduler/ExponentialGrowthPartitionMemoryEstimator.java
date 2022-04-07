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
import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.spi.ErrorCode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTaskMemoryEstimationQuantile;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTaskMemoryGrowthFactor;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;

public class ExponentialGrowthPartitionMemoryEstimator
        implements PartitionMemoryEstimator
{
    private final TDigest memoryUsageDistribution = new TDigest();

    @Override
    public MemoryRequirements getInitialMemoryRequirements(Session session, DataSize defaultMemoryLimit)
    {
        return new MemoryRequirements(
                Ordering.natural().max(defaultMemoryLimit, getEstimatedMemoryUsage(session)),
                false);
    }

    @Override
    public MemoryRequirements getNextRetryMemoryRequirements(Session session, MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, ErrorCode errorCode)
    {
        DataSize previousMemory = previousMemoryRequirements.getRequiredMemory();

        // start with the maximum of previously used memory and actual usage
        DataSize newMemory = Ordering.natural().max(peakMemoryUsage, previousMemory);
        if (isOutOfMemoryError(errorCode)) {
            // multiply if we hit an oom error
            double growthFactor = getFaultTolerantExecutionTaskMemoryGrowthFactor(session);
            newMemory = DataSize.of((long) (newMemory.toBytes() * growthFactor), DataSize.Unit.BYTE);
        }

        // if we are still below current estimate for new partition let's bump further
        newMemory = Ordering.natural().max(newMemory, getEstimatedMemoryUsage(session));

        return new MemoryRequirements(newMemory, false);
    }

    private boolean isOutOfMemoryError(ErrorCode errorCode)
    {
        return EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode().equals(errorCode) // too many tasks from single query on a node
                || CLUSTER_OUT_OF_MEMORY.toErrorCode().equals(errorCode); // too many tasks in general on a node
    }

    @Override
    public synchronized void registerPartitionFinished(Session session, MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, boolean success, Optional<ErrorCode> errorCode)
    {
        if (success) {
            memoryUsageDistribution.add(peakMemoryUsage.toBytes());
        }
        if (!success && errorCode.isPresent() && isOutOfMemoryError(errorCode.get())) {
            double growthFactor = getFaultTolerantExecutionTaskMemoryGrowthFactor(session);
            // take previousRequiredBytes into account when registering failure on oom. It is conservative hence safer (and in-line with getNextRetryMemoryRequirements)
            long previousRequiredBytes = previousMemoryRequirements.getRequiredMemory().toBytes();
            long previousPeakBytes = peakMemoryUsage.toBytes();
            memoryUsageDistribution.add(Math.max(previousRequiredBytes, previousPeakBytes) * growthFactor);
        }
    }

    private synchronized DataSize getEstimatedMemoryUsage(Session session)
    {
        double estimationQuantile = getFaultTolerantExecutionTaskMemoryEstimationQuantile(session);
        double estimation = memoryUsageDistribution.valueAt(estimationQuantile);
        if (Double.isNaN(estimation)) {
            return DataSize.ofBytes(0);
        }
        return DataSize.ofBytes((long) estimation);
    }
}
