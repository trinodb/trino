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
package io.trino.memory.context;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.connector.MemoryContext;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

final class MemoryContextReservationHandler
        implements MemoryReservationHandler
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();
    // Propagating every page-sized fluctuation to the operator memory context walks the
    // whole context tree from inside the reader's decode loop. Following the strategy of
    // CoarseGrainLocalMemoryContext, round usage up to this granule and report only when
    // the rounded value changes, preferring over-counting over under-counting.
    private static final long REPORT_GRANULARITY = 1 << 20;
    private static final long GRANULARITY_MASK = ~(REPORT_GRANULARITY - 1);

    private final MemoryContext memoryContext;

    @GuardedBy("this")
    private long memoryUsage;

    @GuardedBy("this")
    private long reportedUsage;

    public MemoryContextReservationHandler(MemoryContext memoryContext)
    {
        this.memoryContext = memoryContext;
    }

    @Override
    public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
    {
        if (delta != 0) {
            synchronized (this) {
                memoryUsage += delta;
                long roundedUsage = roundUpToGranularity(memoryUsage);
                if (roundedUsage != reportedUsage) {
                    memoryContext.setBytes(roundedUsage);
                    reportedUsage = roundedUsage;
                }
            }
        }
        return NOT_BLOCKED;
    }

    private static long roundUpToGranularity(long bytes)
    {
        long masked = bytes & GRANULARITY_MASK;
        return masked == bytes ? masked : masked + REPORT_GRANULARITY;
    }

    @Override
    public boolean tryReserveMemory(String allocationTag, long delta)
    {
        ListenableFuture<Void> _ = reserveMemory(allocationTag, delta);
        return true;
    }
}
