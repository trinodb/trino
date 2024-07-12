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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ThresholdLocalMemoryContext
        implements LocalMemoryContext
{
    public static final long DEFAULT_SYNC_THRESHOLD = 65536;

    private final LocalMemoryContext delegate;
    private final long syncThreshold;
    private long syncedBytes;
    private long currentBytes;

    public ThresholdLocalMemoryContext(LocalMemoryContext delegate)
    {
        this(delegate, DEFAULT_SYNC_THRESHOLD);
    }

    public ThresholdLocalMemoryContext(LocalMemoryContext delegate, long syncThreshold)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        checkArgument(syncThreshold > 0, "syncThreshold must be greater than 0");
        this.syncThreshold = syncThreshold;
    }

    @Override
    public long getBytes()
    {
        return currentBytes;
    }

    @Override
    public ListenableFuture<Void> setBytes(long bytes)
    {
        if (bytes < syncedBytes || bytes - syncedBytes > syncThreshold) {
            currentBytes = bytes;
            syncedBytes = bytes;
            return delegate.setBytes(bytes);
        }
        // bytes - syncBytes <= syncThreshold
        currentBytes = bytes;
        return Futures.immediateVoidFuture();
    }

    @Override
    public boolean trySetBytes(long bytes)
    {
        if (bytes < syncedBytes || bytes - syncedBytes > syncThreshold) {
            if (delegate.trySetBytes(bytes)) {
                currentBytes = bytes;
                syncedBytes = bytes;
                return true;
            }
            return false;
        }

        currentBytes = bytes;
        return true;
    }

    @Override
    public void close()
    {
        delegate.close();
        currentBytes = 0;
        syncedBytes = 0;
    }

    public void sync()
    {
        delegate.setBytes(currentBytes);
        syncedBytes = currentBytes;
    }
}
