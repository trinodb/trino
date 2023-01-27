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
package io.trino.execution.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.TDigest;
import io.trino.memory.context.LocalMemoryContext;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

/**
 * OutputBufferMemoryManager will block when any condition below holds
 * - the number of buffered bytes exceeds maxBufferedBytes and blockOnFull is true
 * - the memory pool is exhausted
 */
@ThreadSafe
class OutputBufferMemoryManager
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final long maxBufferedBytes;
    private final AtomicLong bufferedBytes = new AtomicLong();
    private final AtomicLong peakMemoryUsage = new AtomicLong();

    @GuardedBy("this")
    private boolean closed;
    @Nullable
    @GuardedBy("this")
    private SettableFuture<Void> bufferBlockedFuture;
    @GuardedBy("this")
    private ListenableFuture<Void> blockedOnMemory = NOT_BLOCKED;

    private final Ticker ticker = Ticker.systemTicker();

    private final AtomicBoolean blockOnFull = new AtomicBoolean(true);

    private final Supplier<LocalMemoryContext> memoryContextSupplier;
    private final Executor notificationExecutor;

    @GuardedBy("this")
    private final TDigest bufferUtilization = new TDigest();
    @GuardedBy("this")
    private long lastBufferUtilizationRecordTime = -1;
    @GuardedBy("this")
    private double lastBufferUtilization;

    public OutputBufferMemoryManager(long maxBufferedBytes, Supplier<LocalMemoryContext> memoryContextSupplier, Executor notificationExecutor)
    {
        requireNonNull(memoryContextSupplier, "memoryContextSupplier is null");
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be > 0");
        this.maxBufferedBytes = maxBufferedBytes;
        this.memoryContextSupplier = Suppliers.memoize(memoryContextSupplier::get);
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.lastBufferUtilization = 0;
    }

    public void updateMemoryUsage(long bytesAdded)
    {
        // If the memoryContext doesn't exist, the task is probably already
        // aborted, so we can just return (see the comment in getMemoryContextOrNull()).
        LocalMemoryContext memoryContext = getMemoryContextOrNull();
        if (memoryContext == null) {
            return;
        }

        ListenableFuture<Void> waitForMemory = null;
        SettableFuture<Void> notifyUnblocked = null;
        long currentBufferedBytes;
        synchronized (this) {
            // If closed is true, that means the task is completed. In that state,
            // the output buffers already ignore the newly added pages, and therefore
            // we can also safely ignore any calls after OutputBufferMemoryManager is closed.
            if (closed) {
                return;
            }

            currentBufferedBytes = bufferedBytes.updateAndGet(bytes -> {
                long result = bytes + bytesAdded;
                checkArgument(result >= 0, "bufferedBytes (%s) plus delta (%s) would be negative", bytes, bytesAdded);
                return result;
            });
            ListenableFuture<Void> blockedOnMemory = memoryContext.setBytes(currentBufferedBytes);
            if (!blockedOnMemory.isDone()) {
                if (this.blockedOnMemory != blockedOnMemory) {
                    this.blockedOnMemory = blockedOnMemory;
                    waitForMemory = blockedOnMemory; // only register a callback when blocked and the future is different
                }
            }
            else {
                this.blockedOnMemory = NOT_BLOCKED;
                if (currentBufferedBytes <= maxBufferedBytes || !blockOnFull.get()) {
                    // Complete future in a new thread to avoid making a callback on the caller thread.
                    // This make is easier for callers to use this class since they can update the memory
                    // usage while holding locks.
                    notifyUnblocked = this.bufferBlockedFuture;
                    this.bufferBlockedFuture = null;
                }
            }
            recordBufferUtilization();
        }
        peakMemoryUsage.accumulateAndGet(currentBufferedBytes, Math::max);
        // Notify listeners outside of the critical section
        notifyListener(notifyUnblocked);
        if (waitForMemory != null) {
            waitForMemory.addListener(this::onMemoryAvailable, notificationExecutor);
        }
    }

    private synchronized void recordBufferUtilization()
    {
        long recordTime = ticker.read();
        if (lastBufferUtilizationRecordTime != -1) {
            bufferUtilization.add(lastBufferUtilization, (double) recordTime - this.lastBufferUtilizationRecordTime);
        }
        double utilization = getUtilization();
        // skip recording of buffer utilization until data is put into buffer
        if (lastBufferUtilizationRecordTime != -1 || utilization != 0.0) {
            lastBufferUtilizationRecordTime = recordTime;
            lastBufferUtilization = utilization;
        }
    }

    public synchronized ListenableFuture<Void> getBufferBlockedFuture()
    {
        if (bufferBlockedFuture == null) {
            if (blockedOnMemory.isDone() && !isBufferFull()) {
                return NOT_BLOCKED;
            }
            bufferBlockedFuture = SettableFuture.create();
        }
        return bufferBlockedFuture;
    }

    public void setNoBlockOnFull()
    {
        SettableFuture<Void> future = null;
        synchronized (this) {
            blockOnFull.set(false);

            if (blockedOnMemory.isDone()) {
                future = this.bufferBlockedFuture;
                this.bufferBlockedFuture = null;
            }
        }
        // Complete future in a new thread to avoid making a callback on the caller thread.
        notifyListener(future);
    }

    public long getBufferedBytes()
    {
        return bufferedBytes.get();
    }

    public double getUtilization()
    {
        return bufferedBytes.get() / (double) maxBufferedBytes;
    }

    public synchronized TDigest getUtilizationHistogram()
    {
        // always get most up to date histogram
        recordBufferUtilization();
        return TDigest.copyOf(bufferUtilization);
    }

    public boolean isOverutilized()
    {
        return isBufferFull();
    }

    private boolean isBufferFull()
    {
        return bufferedBytes.get() > maxBufferedBytes && blockOnFull.get();
    }

    @VisibleForTesting
    void onMemoryAvailable()
    {
        // Check if the buffer is full before synchronizing and skip notifying listeners
        if (isBufferFull()) {
            return;
        }

        SettableFuture<Void> future;
        synchronized (this) {
            // re-check after synchronizing and ensure the current memory future is completed
            if (isBufferFull() || !blockedOnMemory.isDone()) {
                return;
            }
            future = this.bufferBlockedFuture;
            this.bufferBlockedFuture = null;
        }
        // notify listeners if the buffer is not full
        notifyListener(future);
    }

    public long getPeakMemoryUsage()
    {
        return peakMemoryUsage.get();
    }

    public synchronized void close()
    {
        updateMemoryUsage(-bufferedBytes.get());
        LocalMemoryContext memoryContext = getMemoryContextOrNull();
        if (memoryContext != null) {
            memoryContext.close();
        }
        closed = true;
    }

    private void notifyListener(@Nullable SettableFuture<Void> future)
    {
        if (future != null) {
            notificationExecutor.execute(() -> future.set(null));
        }
    }

    @Nullable
    private LocalMemoryContext getMemoryContextOrNull()
    {
        try {
            return memoryContextSupplier.get();
        }
        catch (RuntimeException ignored) {
            // This is possible with races, e.g., a task is created and then immediately aborted,
            // so that the task context hasn't been created yet (as a result there's no memory context available).
            return null;
        }
    }
}
