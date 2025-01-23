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
package io.trino.plugin.bigquery;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import static java.util.Objects.requireNonNull;

public class BigQueryArrowBufferAllocator
{
    private static final Logger log = Logger.get(BigQueryArrowBufferAllocator.class);

    private final long maximumAllocation;
    private final BufferAllocator rootAllocator;

    @Inject
    public BigQueryArrowBufferAllocator(BigQueryArrowConfig config)
    {
        this.maximumAllocation = requireNonNull(config, "config is null").getMaxAllocation().toBytes();
        this.rootAllocator = new RootAllocator(maximumAllocation);
    }

    public BufferAllocator newChildAllocator(BigQuerySplit split)
    {
        return rootAllocator.newChildAllocator(
                split.streamName(),
                new RetryingAllocationListener(split.streamName()),
                split.dataSize().orElse(0),
                maximumAllocation);
    }

    private static class RetryingAllocationListener
            implements AllocationListener
    {
        private final String name;

        private RetryingAllocationListener(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public boolean onFailedAllocation(long size, AllocationOutcome outcome)
        {
            log.warn("Failed to allocate %d bytes for allocator '%s' due to %s", size, name, outcome.getStatus());
            outcome.getDetails().ifPresent(details -> {
                log.warn("Allocation failure details: %s", details.toString());
            });
            return false;
        }
    }
}
