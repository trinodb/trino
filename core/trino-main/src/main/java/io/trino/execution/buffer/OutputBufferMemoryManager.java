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
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.stats.TDigest;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.base.util.Lazy;
import jakarta.annotation.Nullable;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

/**
 * OutputBufferMemoryManager will block when any condition below holds
 * - the size of the buffered data exceeds maxBufferedBytes and blockOnFull is true
 * - the memory pool is exhausted
 * <p>
 * Memory is accounted at the retained size of the buffered pages, while blocking is
 * driven by their data size. The two differ for raw pages buffered for a consumer on
 * the same node: sizing flow control by the retained size would penalize them for
 * memory over-allocated by the producing operators.
 */
@ThreadSafe
final class OutputBufferMemoryManager
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final long maxBufferedBytes;
    private final AtomicLong bufferedRetainedBytes = new AtomicLong();
    private final AtomicLong bufferedSizeInBytes = new AtomicLong();
    private final AtomicLong peakMemoryUsage = new AtomicLong();

    @GuardedBy("this")
    private boolean closed;
    // guarded by "this" for updates
    @Nullable
    private volatile SettableFuture<Void> bufferBlockedFuture;
    // guarded by "this" for updates
    private volatile ListenableFuture<Void> blockedOnMemory = NOT_BLOCKED;

    private final Ticker ticker = Ticker.systemTicker();

    private final AtomicBoolean blockOnFull = new AtomicBoolean(true);

    private final Lazy<LocalMemoryContext> memoryContextSupplier;
    private final Executor notificationExecutor;

    @GuardedBy("this")
    private final TDigest bufferUtilization = new TDigest();
    @GuardedBy("this")
    private long lastBufferUtilizationRecordTime;
    @GuardedBy("this")
    private double lastBufferUtilization;

    public OutputBufferMemoryManager(long maxBufferedBytes, Lazy<LocalMemoryContext> memoryContextSupplier, Executor notificationExecutor)
    {
        requireNonNull(memoryContextSupplier, "memoryContextSupplier is null");
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be > 0");
        this.maxBufferedBytes = maxBufferedBytes;
        this.memoryContextSupplier = requireNonNull(memoryContextSupplier, "memoryContextSupplier is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.lastBufferUtilizationRecordTime = ticker.read();
        this.lastBufferUtilization = 0;
    }

    public void updateMemoryUsage(long retainedBytesAdded, long sizeInBytesAdded)
    {
        // If the memoryContext doesn't exist, the task is probably already
        // aborted, so we can just return (see the comment in getMemoryContextOrNull()).
        LocalMemoryContext memoryContext = getMemoryContextOrNull();
        if (memoryContext == null) {
            return;
        }

        ListenableFuture<Void> waitForMemory = null;
        SettableFuture<Void> notifyUnblocked = null;
        final long currentRetainedBytes;
        synchronized (this) {
            // If closed is true, that means the task is completed. In that state,
            // the output buffers already ignore the newly added pages, and therefore
            // we can also safely ignore any calls after OutputBufferMemoryManager is closed.
            if (closed) {
                return;
            }

            currentRetainedBytes = addAndCheck(bufferedRetainedBytes, retainedBytesAdded, "bufferedRetainedBytes");
            long currentSizeInBytes = addAndCheck(bufferedSizeInBytes, sizeInBytesAdded, "bufferedSizeInBytes");
            ListenableFuture<Void> blockedOnMemory = memoryContext.setBytes(currentRetainedBytes);
            if (!blockedOnMemory.isDone()) {
                if (this.blockedOnMemory != blockedOnMemory) {
                    this.blockedOnMemory = blockedOnMemory;
                    waitForMemory = blockedOnMemory; // only register a callback when blocked and the future is different
                }
            }
            else {
                this.blockedOnMemory = NOT_BLOCKED;
                if (currentSizeInBytes <= maxBufferedBytes || !blockOnFull.get()) {
                    // Complete future in a new thread to avoid making a callback on the caller thread.
                    // This make is easier for callers to use this class since they can update the memory
                    // usage while holding locks.
                    notifyUnblocked = this.bufferBlockedFuture;
                    this.bufferBlockedFuture = null;
                }
            }
            recordBufferUtilization(currentSizeInBytes);
        }
        // Reduce contention by reading first and only updating if the new value might become the maximum (uncommon)
        if (currentRetainedBytes > peakMemoryUsage.get()) {
            peakMemoryUsage.accumulateAndGet(currentRetainedBytes, Math::max);
        }
        // Notify listeners outside of the critical section
        notifyListener(notifyUnblocked);
        if (waitForMemory != null) {
            waitForMemory.addListener(this::onMemoryAvailable, notificationExecutor);
        }
    }

    private static long addAndCheck(AtomicLong total, long delta, String name)
    {
        return total.accumulateAndGet(delta, (current, added) -> {
            long result = current + added;
            checkArgument(result >= 0, "%s (%s) plus delta (%s) would be negative", name, current, added);
            return result;
        });
    }

    private synchronized void recordBufferUtilization(long currentBufferedBytes)
    {
        long recordTime = ticker.read();
        bufferUtilization.add(lastBufferUtilization, (double) recordTime - this.lastBufferUtilizationRecordTime);
        lastBufferUtilizationRecordTime = recordTime;
        lastBufferUtilization = getUtilization(currentBufferedBytes);
    }

    public ListenableFuture<Void> getBufferBlockedFuture()
    {
        ListenableFuture<Void> bufferBlockedFuture = this.bufferBlockedFuture;
        if (bufferBlockedFuture == null) {
            if (blockedOnMemory.isDone() && !isBufferFull()) {
                return NOT_BLOCKED;
            }
            synchronized (this) {
                if (this.bufferBlockedFuture == null) {
                    if (blockedOnMemory.isDone() && !isBufferFull()) {
                        return NOT_BLOCKED;
                    }
                    this.bufferBlockedFuture = SettableFuture.create();
                }
                return this.bufferBlockedFuture;
            }
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
        return bufferedRetainedBytes.get();
    }

    public double getUtilization()
    {
        return getUtilization(bufferedSizeInBytes.get());
    }

    private double getUtilization(long currentBufferedBytes)
    {
        return currentBufferedBytes / (double) maxBufferedBytes;
    }

    public synchronized TDigest getUtilizationHistogram()
    {
        // always get most up to date histogram
        recordBufferUtilization(bufferedSizeInBytes.get());
        return TDigest.copyOf(bufferUtilization);
    }

    public boolean isOverutilized()
    {
        return isBufferFull();
    }

    private boolean isBufferFull()
    {
        return bufferedSizeInBytes.get() > maxBufferedBytes && blockOnFull.get();
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
        updateMemoryUsage(-bufferedRetainedBytes.get(), -bufferedSizeInBytes.get());
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
        catch (RuntimeException _) {
            // This is possible with races, e.g., a task is created and then immediately aborted,
            // so that the task context hasn't been created yet (as a result there's no memory context available).
            return null;
        }
    }
}
