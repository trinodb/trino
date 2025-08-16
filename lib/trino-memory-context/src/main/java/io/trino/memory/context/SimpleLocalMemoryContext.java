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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class SimpleLocalMemoryContext
        implements LocalMemoryContext
{
    private static final ListenableFuture<Void> NOT_BLOCKED = immediateVoidFuture();

    private final AbstractAggregatedMemoryContext parentMemoryContext;
    private final String allocationTag;

    @GuardedBy("this")
    private long usedBytes;
    @GuardedBy("this")
    private boolean closed;

    public SimpleLocalMemoryContext(AggregatedMemoryContext parentMemoryContext, String allocationTag)
    {
        verify(parentMemoryContext instanceof AbstractAggregatedMemoryContext);
        this.parentMemoryContext = (AbstractAggregatedMemoryContext) requireNonNull(parentMemoryContext, "parentMemoryContext is null");
        this.allocationTag = requireNonNull(allocationTag, "allocationTag is null");
    }

    @Override
    public synchronized long getBytes()
    {
        return usedBytes;
    }

    @Override
    public synchronized ListenableFuture<Void> setBytes(long bytes)
    {
        checkState(!closed, "SimpleLocalMemoryContext is already closed");
        checkArgument(bytes >= 0, "bytes cannot be negative");

        if (bytes == usedBytes) {
            return NOT_BLOCKED;
        }

        // update the parent first as it may throw a runtime exception (e.g., ExceededMemoryLimitException)
        ListenableFuture<Void> future = parentMemoryContext.updateBytes(allocationTag, bytes - usedBytes);
        usedBytes = bytes;
        return future;
    }

    @Override
    public synchronized ListenableFuture<Void> addBytes(long delta)
    {
        return setBytes(usedBytes + delta);
    }

    @Override
    public synchronized boolean trySetBytes(long bytes)
    {
        checkState(!closed, "SimpleLocalMemoryContext is already closed");
        checkArgument(bytes >= 0, "bytes cannot be negative");
        long delta = bytes - usedBytes;
        if (parentMemoryContext.tryUpdateBytes(allocationTag, delta)) {
            usedBytes = bytes;
            return true;
        }
        return false;
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        parentMemoryContext.updateBytes(allocationTag, -usedBytes);
        usedBytes = 0;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("allocationTag", allocationTag)
                .add("usedBytes", usedBytes)
                .toString();
    }
}
