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

import io.airlift.units.DataSize;
import io.trino.spi.ErrorCode;

import java.util.Optional;

public class NoMemoryPartitionMemoryEstimator
        implements PartitionMemoryEstimator
{
    public static final NoMemoryPartitionMemoryEstimator INSTANCE = new NoMemoryPartitionMemoryEstimator();

    private NoMemoryPartitionMemoryEstimator() {}

    @Override
    public MemoryRequirements getInitialMemoryRequirements()
    {
        return new MemoryRequirements(DataSize.ofBytes(0));
    }

    @Override
    public MemoryRequirements getNextRetryMemoryRequirements(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, ErrorCode errorCode)
    {
        return new MemoryRequirements(DataSize.ofBytes(0));
    }

    @Override
    public void registerPartitionFinished(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, boolean success, Optional<ErrorCode> errorCode) {}
}
