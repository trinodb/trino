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
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.NoopBlob;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class MemoryBlobCache
        implements BlobCache
{
    private final Cache<CacheKey, Slice> cache;
    private final int maxContentLengthBytes;
    private final AtomicLong largeFileSkippedCount = new AtomicLong();
    private final MemoryBlobCacheStats discardedStats = new MemoryBlobCacheStats();

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
                .weigher((Weigher<CacheKey, Slice>) (key, value) -> toIntExact(estimatedSizeOf(key.components(), SizeOf::estimatedSizeOf) + value.getRetainedSize()))
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
        return get(key, source, discardedStats);
    }

    public Blob get(CacheKey key, BlobSource source, MemoryBlobCacheStats stats)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(source, "source is null");
        requireNonNull(stats, "stats is null");
        try {
            CachedContent content = getOrLoad(key, source, stats);
            if (content == null) {
                // The pass-through blob owns the source and closes it with the blob
                return new NoopBlob(source);
            }
            source.close();
            // Only the lookup that populated the entry fetched anything from the source
            return new MemoryBlob(content.data(), content.loaded() ? content.data().length() : 0);
        }
        catch (Throwable e) {
            // The cache owns the source until a blob is returned, so it must not stay open
            // when this method fails
            closeAllSuppress(e, source);
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tryInvalidate(CacheKey prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<CacheKey> matching = cache.asMap().keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .collect(toImmutableList());
        cache.invalidateAll(matching);
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

    /**
     * Fraction of lookups served from the cache, over the cache's lifetime and all catalogs.
     */
    @Managed
    public double getHitRatio()
    {
        return cache.stats().hitRate();
    }

    @VisibleForTesting
    boolean isCached(CacheKey key)
    {
        return cache.getIfPresent(key) != null;
    }

    /**
     * Content of a cache entry, and whether this lookup is the one that read it from the source.
     */
    private record CachedContent(Slice data, boolean loaded) {}

    private CachedContent getOrLoad(CacheKey key, BlobSource source, MemoryBlobCacheStats stats)
            throws IOException
    {
        Slice cached = cache.getIfPresent(key);
        if (cached != null) {
            stats.recordHit();
            return new CachedContent(cached, false);
        }
        long length = source.length();
        if (length > maxContentLengthBytes) {
            largeFileSkippedCount.incrementAndGet();
            stats.recordLargeFileSkipped();
            return null;
        }
        stats.recordMiss();
        try {
            // The loader does not run when a concurrent lookup populates the entry first
            boolean[] loaded = new boolean[1];
            Slice data = cache.get(key, () -> {
                loaded[0] = true;
                return load(source, length);
            });
            return new CachedContent(data, loaded[0]);
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

    private static Slice load(BlobSource source, long length)
            throws IOException
    {
        byte[] buffer = new byte[toIntExact(length)];
        source.readFully(0, buffer, 0, buffer.length);
        return Slices.wrappedBuffer(buffer);
    }
}
