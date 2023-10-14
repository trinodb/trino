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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public interface PartitionMemoryEstimator
{
    MemoryRequirements getInitialMemoryRequirements();

    MemoryRequirements getNextRetryMemoryRequirements(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, ErrorCode errorCode);

    void registerPartitionFinished(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, boolean success, Optional<ErrorCode> errorCode);

    class MemoryRequirements
    {
        private final DataSize requiredMemory;

        MemoryRequirements(DataSize requiredMemory)
        {
            this.requiredMemory = requireNonNull(requiredMemory, "requiredMemory is null");
        }

        public DataSize getRequiredMemory()
        {
            return requiredMemory;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MemoryRequirements that = (MemoryRequirements) o;
            return Objects.equals(requiredMemory, that.requiredMemory);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(requiredMemory);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("requiredMemory", requiredMemory)
                    .toString();
        }
    }
}
