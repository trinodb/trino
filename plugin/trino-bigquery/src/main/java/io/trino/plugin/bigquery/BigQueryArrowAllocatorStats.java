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

import io.airlift.stats.CounterStat;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class BigQueryArrowAllocatorStats
        implements AllocationListener
{
    private final CounterStat preAllocatedMemory = new CounterStat();
    private final CounterStat allocatedMemory = new CounterStat();
    private final CounterStat releasedMemory = new CounterStat();
    private final CounterStat failedMemory = new CounterStat();

    @Managed
    @Nested
    public CounterStat getAllocatedMemory()
    {
        return allocatedMemory;
    }

    @Managed
    @Nested
    public CounterStat getReleasedMemory()
    {
        return releasedMemory;
    }

    @Managed
    @Nested
    public CounterStat getPreAllocatedMemory()
    {
        return preAllocatedMemory;
    }

    @Managed
    @Nested
    public CounterStat getFailedMemory()
    {
        return failedMemory;
    }

    @Override
    public void onAllocation(long size)
    {
        allocatedMemory.update(size);
    }

    @Override
    public void onRelease(long size)
    {
        releasedMemory.update(size);
    }

    @Override
    public void onPreAllocation(long size)
    {
        preAllocatedMemory.update(size);
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome)
    {
        failedMemory.update(size);
        return false;
    }
}
