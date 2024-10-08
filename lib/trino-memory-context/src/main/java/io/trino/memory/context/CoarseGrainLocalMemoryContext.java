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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 *  This class prevents contention in the memory tracking system by coarsening the granularity of memory tracking.
 *  In this paradigm, the most small incremental increases at the byte granularity will not actually result
 *  in a different coarse granularity value, so no reporting into memory tracking is necessary
 */
public class CoarseGrainLocalMemoryContext
        implements LocalMemoryContext
{
    public static final long DEFAULT_GRANULARITY = 65536;

    private final LocalMemoryContext delegate;
    private final long granularity;
    private final long mask;
    @GuardedBy("this")
    private long currentBytes;

    public CoarseGrainLocalMemoryContext(LocalMemoryContext delegate)
    {
        this(delegate, DEFAULT_GRANULARITY);
    }

    public CoarseGrainLocalMemoryContext(LocalMemoryContext delegate, long granularity)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        checkArgument(granularity > 0, "granularity must be greater than 0");
        checkArgument((granularity & (granularity - 1)) == 0, "granularity must be a power of 2");
        this.granularity = granularity;
        this.mask = ~(granularity - 1);
    }

    @Override
    public synchronized long getBytes()
    {
        return currentBytes;
    }

    @Override
    public synchronized ListenableFuture<Void> setBytes(long bytes)
    {
        long roundedUpBytes = roundUpToNearest(bytes);
        if (roundedUpBytes != currentBytes) {
            currentBytes = roundedUpBytes;
            return delegate.setBytes(currentBytes);
        }
        return Futures.immediateVoidFuture();
    }

    @Override
    public synchronized boolean trySetBytes(long bytes)
    {
        long roundedUpBytes = roundUpToNearest(bytes);
        if (roundedUpBytes != currentBytes) {
            if (delegate.trySetBytes(roundedUpBytes)) {
                currentBytes = roundedUpBytes;
                return true;
            }
            return false;
        }
        return true;
    }

    @Override
    public synchronized void close()
    {
        delegate.close();
        currentBytes = 0;
    }

    @VisibleForTesting
    long roundUpToNearest(long bytes)
    {
        long masked = bytes & mask;
        return masked == bytes ? masked : masked + granularity;
    }
}
