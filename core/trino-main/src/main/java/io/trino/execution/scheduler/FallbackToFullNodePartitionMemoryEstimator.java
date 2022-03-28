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

import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.spi.ErrorCode;

import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;

public class FallbackToFullNodePartitionMemoryEstimator
        implements PartitionMemoryEstimator
{
    // temporarily express full-node requirement as huge amount of memory
    public static final DataSize FULL_NODE_MEMORY = DataSize.of(512, DataSize.Unit.GIGABYTE);

    private static final MemoryRequirements FULL_NODE_MEMORY_REQUIREMENTS = new MemoryRequirements(FULL_NODE_MEMORY, true);

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
        if (shouldRescheduleWithFullNode(errorCode)) {
            return FULL_NODE_MEMORY_REQUIREMENTS;
        }
        return previousMemoryRequirements;
    }

    private boolean shouldRescheduleWithFullNode(ErrorCode errorCode)
    {
        return EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode().equals(errorCode) // too many tasks from single query on a node
                || CLUSTER_OUT_OF_MEMORY.toErrorCode().equals(errorCode); // too many tasks in general on a node
    }
}
