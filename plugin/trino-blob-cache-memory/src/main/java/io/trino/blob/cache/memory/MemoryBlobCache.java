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
package io.trino.blob.cache.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.PassThroughBlob;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class MemoryBlobCache
        implements BlobCache
{
    private final Cache<CacheKey, Optional<Slice>> cache;
    private final int maxContentLengthBytes;
    private final AtomicLong largeFileSkippedCount = new AtomicLong();

    @Inject
    public MemoryBlobCache(MemoryBlobCacheConfig config)
    {
        this(config.getCacheTtl(), config.getMaxSize(), config.getMaxContentLength());
    }

    private MemoryBlobCache(Duration expireAfterWrite, DataSize maxSize, DataSize maxContentLength)
    {
        checkArgument(maxContentLength.compareTo(DataSize.of(1, GIGABYTE)) <= 0, "maxContentLength must be less than or equal to 1GB");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .weigher((Weigher<CacheKey, Optional<Slice>>) (key, value) -> toIntExact(estimatedSizeOf(key.key()) + sizeOf(value, Slice::getRetainedSize)))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.maxContentLengthBytes = toIntExact(maxContentLength.toBytes());
    }

    @Override
    public Blob get(CacheKey key, BlobSource source)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(source, "source is null");
        Optional<Slice> cachedEntry = getOrLoad(key, source);
        if (cachedEntry.isEmpty()) {
            return new PassThroughBlob(source);
        }
        return new MemoryBlob(cachedEntry.get());
    }

    @Override
    public void invalidate(CacheKey key)
    {
        requireNonNull(key, "key is null");
        cache.invalidate(key);
    }

    @Override
    public void invalidate(Collection<CacheKey> keys)
    {
        requireNonNull(keys, "keys is null");
        cache.invalidateAll(keys);
    }

    public void invalidatePrefix(String prefix)
    {
        List<CacheKey> expired = cache.asMap().keySet().stream()
                .filter(k -> k.key().startsWith(prefix))
                .collect(toImmutableList());
        cache.invalidateAll(expired);
    }

    @Managed
    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Managed
    public long getLargeFileSkippedCount()
    {
        return largeFileSkippedCount.get();
    }

    public long size()
    {
        return cache.size();
    }

    public long evictionCount()
    {
        return cache.stats().evictionCount();
    }

    @VisibleForTesting
    boolean isCached(CacheKey key)
    {
        Optional<Slice> cachedEntry = cache.getIfPresent(key);
        return cachedEntry != null && cachedEntry.isPresent();
    }

    private Optional<Slice> getOrLoad(CacheKey key, BlobSource source)
            throws IOException
    {
        Optional<Slice> cached = cache.getIfPresent(key);
        if (cached != null && cached.isPresent()) {
            return cached;
        }
        long length = source.length();
        if (length > maxContentLengthBytes) {
            largeFileSkippedCount.incrementAndGet();
            return Optional.empty();
        }
        try {
            return cache.get(key, () -> load(source, length));
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException(cause);
        }
    }

    private Optional<Slice> load(BlobSource source, long length)
            throws IOException
    {
        // Populate via tail-anchored read so callers (such as Iceberg's use_file_size_from_metadata)
        // that pass a length not matching the storage object still see the bytes at the actual end of
        // the file, matching the prior MemoryFileSystemCache behavior.
        byte[] buffer = new byte[toIntExact(length)];
        source.readTail(buffer, 0, buffer.length);
        return Optional.of(Slices.wrappedBuffer(buffer));
    }
}
