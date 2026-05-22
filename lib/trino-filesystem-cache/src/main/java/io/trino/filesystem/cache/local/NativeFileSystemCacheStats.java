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
package io.trino.filesystem.cache.local;

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class NativeFileSystemCacheStats
{
    private final AtomicLong cacheReadBytes = new AtomicLong();
    private final AtomicLong externalReadBytes = new AtomicLong();
    private final AtomicLong cacheReads = new AtomicLong();
    private final AtomicLong externalReads = new AtomicLong();
    private final AtomicLong cacheWrites = new AtomicLong();
    private final AtomicLong cacheWriteFailures = new AtomicLong();

    void recordCacheRead(long bytes)
    {
        cacheReads.incrementAndGet();
        cacheReadBytes.addAndGet(bytes);
    }

    void recordExternalRead(long bytes)
    {
        externalReads.incrementAndGet();
        externalReadBytes.addAndGet(bytes);
    }

    void recordCacheWrite()
    {
        cacheWrites.incrementAndGet();
    }

    void recordCacheWriteFailure()
    {
        cacheWriteFailures.incrementAndGet();
    }

    @Managed
    public long getCacheReadBytes()
    {
        return cacheReadBytes.get();
    }

    @Managed
    public long getExternalReadBytes()
    {
        return externalReadBytes.get();
    }

    @Managed
    public long getCacheReads()
    {
        return cacheReads.get();
    }

    @Managed
    public long getExternalReads()
    {
        return externalReads.get();
    }

    @Managed
    public long getCacheWrites()
    {
        return cacheWrites.get();
    }

    @Managed
    public long getCacheWriteFailures()
    {
        return cacheWriteFailures.get();
    }
}
