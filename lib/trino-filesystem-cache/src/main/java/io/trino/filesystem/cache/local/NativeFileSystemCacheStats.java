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
    private final AtomicLong cacheWriteSkips = new AtomicLong();
    private final AtomicLong cachedBytes = new AtomicLong();
    private final AtomicLong cachedFiles = new AtomicLong();
    private final AtomicLong evictionCount = new AtomicLong();
    private final AtomicLong evictionBytes = new AtomicLong();
    private final AtomicLong evictionFiles = new AtomicLong();
    private final AtomicLong expiredFiles = new AtomicLong();
    private final AtomicLong staleTemporaryFiles = new AtomicLong();
    private final AtomicLong accessRecords = new AtomicLong();

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

    void recordCacheWriteSkip()
    {
        cacheWriteSkips.incrementAndGet();
    }

    void addCachedFile(long bytes)
    {
        cachedFiles.incrementAndGet();
        cachedBytes.addAndGet(bytes);
    }

    void removeCachedFiles(long files, long bytes)
    {
        adjustCachedFiles(-files, -bytes);
    }

    void adjustCachedFiles(long files, long bytes)
    {
        cachedFiles.addAndGet(files);
        cachedBytes.addAndGet(bytes);
    }

    void recordEviction(long files, long bytes)
    {
        evictionCount.incrementAndGet();
        evictionFiles.addAndGet(files);
        evictionBytes.addAndGet(bytes);
    }

    void recordExpiredFile()
    {
        expiredFiles.incrementAndGet();
    }

    void recordStaleTemporaryFile()
    {
        staleTemporaryFiles.incrementAndGet();
    }

    void recordAccess()
    {
        accessRecords.incrementAndGet();
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

    @Managed
    public long getCacheWriteSkips()
    {
        return cacheWriteSkips.get();
    }

    @Managed
    public long getCachedBytes()
    {
        return cachedBytes.get();
    }

    @Managed
    public long getCachedFiles()
    {
        return cachedFiles.get();
    }

    @Managed
    public long getEvictionCount()
    {
        return evictionCount.get();
    }

    @Managed
    public long getEvictionBytes()
    {
        return evictionBytes.get();
    }

    @Managed
    public long getEvictionFiles()
    {
        return evictionFiles.get();
    }

    @Managed
    public long getExpiredFiles()
    {
        return expiredFiles.get();
    }

    @Managed
    public long getStaleTemporaryFiles()
    {
        return staleTemporaryFiles.get();
    }

    @Managed
    public long getAccessRecords()
    {
        return accessRecords.get();
    }
}
