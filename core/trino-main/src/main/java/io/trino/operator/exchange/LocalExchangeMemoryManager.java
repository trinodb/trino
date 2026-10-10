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
package io.trino.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.DataSize;
import jakarta.annotation.Nullable;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

/**
 * Blocks writers when the buffered bytes exceed the limit or the memory
 * reservation is blocked on the memory pool. The reported bytes move to one
 * step above the buffered bytes only when the buffered bytes exceed them or
 * fall two steps below them, so adding and removing pages does not touch the
 * memory reservation shared with other managers between moves.
 */
@ThreadSafe
public class LocalExchangeMemoryManager
{
    static final long DEFAULT_REPORT_STEP_BYTES = DataSize.of(1, MEGABYTE).toBytes();

    private final long maxBufferedBytes;
    private final LocalExchangeMemoryReservation memoryReservation;
    private final long reportStepBytes;
    private final AtomicLong bufferedBytes = new AtomicLong();

    // guarded by "this" for updates
    private volatile long reportedBytes;

    @Nullable
    @GuardedBy("this")
    private SettableFuture<Void> notFullFuture; // null represents "no callback registered"

    public LocalExchangeMemoryManager(long maxBufferedBytes, LocalExchangeMemoryReservation memoryReservation)
    {
        this(maxBufferedBytes, memoryReservation, DEFAULT_REPORT_STEP_BYTES);
    }

    public LocalExchangeMemoryManager(long maxBufferedBytes, LocalExchangeMemoryReservation memoryReservation, long reportStepBytes)
    {
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be > 0");
        checkArgument(reportStepBytes > 0, "reportStepBytes must be > 0");
        this.maxBufferedBytes = maxBufferedBytes;
        this.memoryReservation = requireNonNull(memoryReservation, "memoryReservation is null");
        this.reportStepBytes = reportStepBytes;
    }

    public void updateMemoryUsage(long bytesAdded)
    {
        long bufferedBytes = this.bufferedBytes.addAndGet(bytesAdded);
        long reportedBytes = this.reportedBytes;
        if (bufferedBytes > reportedBytes || bufferedBytes < reportedBytes - 2 * reportStepBytes) {
            reportMemoryUsage();
        }
        // detect the transition from above to below the full boundary
        if (bufferedBytes <= maxBufferedBytes && (bufferedBytes - bytesAdded) > maxBufferedBytes) {
            SettableFuture<Void> future;
            synchronized (this) {
                // if we have no callback waiting, return early
                if (notFullFuture == null) {
                    return;
                }
                future = notFullFuture;
                notFullFuture = null;
            }
            // complete future outside of lock since this can invoke callbacks
            future.set(null);
        }
    }

    private synchronized void reportMemoryUsage()
    {
        // re-check since another thread may have reported
        long bufferedBytes = this.bufferedBytes.get();
        if (bufferedBytes <= reportedBytes && bufferedBytes >= reportedBytes - 2 * reportStepBytes) {
            return;
        }
        long previousReportedBytes = reportedBytes;
        reportedBytes = bufferedBytes + reportStepBytes;
        memoryReservation.updateMemoryUsage(reportedBytes - previousReportedBytes);
    }

    public ListenableFuture<Void> getNotFullFuture()
    {
        if (bufferedBytes.get() <= maxBufferedBytes) {
            return memoryReservation.getMemoryFuture();
        }
        synchronized (this) {
            // Recheck after synchronizing but before creating a real listener
            if (bufferedBytes.get() <= maxBufferedBytes) {
                return memoryReservation.getMemoryFuture();
            }
            // if we are full and no current listener is registered, create one
            if (notFullFuture == null) {
                notFullFuture = SettableFuture.create();
            }
            return notFullFuture;
        }
    }

    public long getBufferedBytes()
    {
        return bufferedBytes.get();
    }
}
