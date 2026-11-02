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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.memory.context.LocalMemoryContext;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

/**
 * Reserves the bytes reported by the memory managers of a local exchange
 * in the memory pool.
 */
@ThreadSafe
public class LocalExchangeMemoryReservation
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final LocalMemoryContext memoryContext;

    @GuardedBy("this")
    private long reservedBytes;
    // guarded by "this" for updates
    private volatile ListenableFuture<Void> memoryFuture = NOT_BLOCKED;
    @GuardedBy("this")
    private boolean closed;

    public LocalExchangeMemoryReservation(LocalMemoryContext memoryContext)
    {
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    public synchronized void updateMemoryUsage(long bytesAdded)
    {
        if (closed) {
            return;
        }
        ListenableFuture<Void> future = memoryContext.setBytes(reservedBytes + bytesAdded);
        reservedBytes += bytesAdded;
        // keep waiting on a pending future until the pool frees memory, even if the reservation shrinks
        if (!future.isDone()) {
            memoryFuture = future;
        }
    }

    /**
     * Returns a future that is done when the memory pool is not blocked.
     */
    public ListenableFuture<Void> getMemoryFuture()
    {
        ListenableFuture<Void> future = memoryFuture;
        if (future.isDone()) {
            return NOT_BLOCKED;
        }
        return future;
    }

    /**
     * Releases the reservation. Later updates are ignored.
     */
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        memoryContext.close();
        reservedBytes = 0;
        memoryFuture = NOT_BLOCKED;
    }
}
