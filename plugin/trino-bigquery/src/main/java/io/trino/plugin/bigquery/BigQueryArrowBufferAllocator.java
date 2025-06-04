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
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import static java.util.Objects.requireNonNull;

public class BigQueryArrowBufferAllocator
{
    private static final Logger log = Logger.get(BigQueryArrowBufferAllocator.class);
    private static final long NO_RESERVATION = 0;

    private final long maximumAllocationPerWorkerThread;
    private final BigQueryArrowAllocatorStats stats;
    private final BufferAllocator rootAllocator;

    @Inject
    public BigQueryArrowBufferAllocator(BigQueryArrowConfig config, BigQueryArrowAllocatorStats stats)
    {
        long estimatedMaxWorkerThreads = (long) Runtime.getRuntime().availableProcessors() * 2;
        long maximumAllocation = requireNonNull(config, "config is null").getMaxAllocation().toBytes();
        this.maximumAllocationPerWorkerThread = maximumAllocation / estimatedMaxWorkerThreads;
        this.stats = requireNonNull(stats, "stats is null");
        this.rootAllocator = new RootAllocator(stats, maximumAllocation);
    }

    public BufferAllocator newChildAllocator(BigQuerySplit split)
    {
        return rootAllocator.newChildAllocator(
                split.streamName(),
                new RetryingAllocationListener(split.streamName(), stats),
                NO_RESERVATION,
                maximumAllocationPerWorkerThread);
    }

    @Managed
    @Flatten
    public BigQueryArrowAllocatorStats getStats()
    {
        return stats;
    }

    @Managed
    public long getCurrentAllocatedMemory()
    {
        return rootAllocator.getAllocatedMemory();
    }

    @Managed
    public long getPeakAllocatedMemory()
    {
        return rootAllocator.getPeakMemoryAllocation();
    }

    @Managed
    public long getCurrentMemoryHeadroom()
    {
        return rootAllocator.getHeadroom();
    }

    @Managed
    public long getChildAllocatorsCount()
    {
        return rootAllocator.getChildAllocators().size();
    }

    private static class RetryingAllocationListener
            implements AllocationListener
    {
        private final String name;
        private final BigQueryArrowAllocatorStats stats;

        private RetryingAllocationListener(String name, BigQueryArrowAllocatorStats stats)
        {
            this.name = requireNonNull(name, "name is null");
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public boolean onFailedAllocation(long size, AllocationOutcome outcome)
        {
            stats.onFailedAllocation(size, outcome);

            log.warn("Failed to allocate %d bytes for allocator '%s' due to %s", size, name, outcome.getStatus());
            outcome.getDetails().ifPresent(details -> {
                log.warn("Allocation failure details: %s", details.toString());
            });
            return false;
        }

        @Override
        public void onPreAllocation(long size)
        {
            stats.onPreAllocation(size);
        }

        @Override
        public void onAllocation(long size)
        {
            stats.onAllocation(size);
        }

        @Override
        public void onRelease(long size)
        {
            stats.onRelease(size);
        }
    }
}
