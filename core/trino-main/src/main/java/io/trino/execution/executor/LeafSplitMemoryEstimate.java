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
package io.trino.execution.executor;

import com.google.errorprone.annotations.ThreadSafe;
import jakarta.annotation.Nullable;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// Highest leaf driver memory sampled for one task.
@ThreadSafe
public class LeafSplitMemoryEstimate
{
    private static final long REFRESH_INTERVAL_NANOS = MILLISECONDS.toNanos(100);

    @Nullable
    private volatile TaskMemory taskMemory;
    private final AtomicLong bytesPerSplit = new AtomicLong();
    private final AtomicLong lastRefreshNanos = new AtomicLong(Long.MIN_VALUE);

    public void setTaskMemory(TaskMemory taskMemory)
    {
        this.taskMemory = requireNonNull(taskMemory, "taskMemory is null");
    }

    @Nullable
    public TaskMemory getTaskMemory()
    {
        return taskMemory;
    }

    // Samples the largest leaf driver memory at most once per refresh interval and returns the highest sample, or zero before the first sample.
    public long sample(long nowNanos)
    {
        TaskMemory memory = taskMemory;
        if (memory == null) {
            return 0;
        }
        long lastRefresh = lastRefreshNanos.get();
        boolean due = lastRefresh == Long.MIN_VALUE || nowNanos - lastRefresh >= REFRESH_INTERVAL_NANOS;
        if (due && lastRefreshNanos.compareAndSet(lastRefresh, nowNanos)) {
            return bytesPerSplit.accumulateAndGet(memory.getLeafDriverMaxBytes(), Math::max);
        }
        return bytesPerSplit.get();
    }
}
