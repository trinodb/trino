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

import java.util.Optional;

public class ConstantPartitionMemoryEstimator
        implements PartitionMemoryEstimator
{
    @Override
    public MemoryRequirements getInitialMemoryRequirements(Session session, DataSize defaultMemoryLimit)
    {
        return new MemoryRequirements(
                defaultMemoryLimit,
                true);
    }

    @Override
    public MemoryRequirements getNextRetryMemoryRequirements(Session session, MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, ErrorCode errorCode)
    {
        return previousMemoryRequirements;
    }

    @Override
    public void registerPartitionFinished(Session session, MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, boolean success, Optional<ErrorCode> errorCode) {}
}
