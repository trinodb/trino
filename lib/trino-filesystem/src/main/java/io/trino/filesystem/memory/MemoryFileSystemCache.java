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
package io.trino.filesystem.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import org.weakref.jmx.Managed;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
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

public final class MemoryFileSystemCache
        implements TrinoFileSystemCache
{
    private final Cache<String, Optional<Slice>> cache;
    private final int maxContentLengthBytes;
    private final AtomicLong largeFileSkippedCount = new AtomicLong();

    @Inject
    public MemoryFileSystemCache(MemoryFileSystemCacheConfig config)
    {
        this(config.getCacheTtl(), config.getMaxSize(), config.getMaxContentLength());
    }

    private MemoryFileSystemCache(Duration expireAfterWrite, DataSize maxSize, DataSize maxContentLength)
    {
        checkArgument(maxContentLength.compareTo(DataSize.of(1, GIGABYTE)) <= 0, "maxContentLength must be less than or equal to 1GB");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .weigher((Weigher<String, Optional<Slice>>) (key, value) -> toIntExact(estimatedSizeOf(key) + sizeOf(value, Slice::getRetainedSize)))
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
        this.maxContentLengthBytes = toIntExact(maxContentLength.toBytes());
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        Optional<Slice> cachedEntry = getOrLoadFromCache(key, delegate);
        if (cachedEntry.isEmpty()) {
            largeFileSkippedCount.incrementAndGet();
            return delegate.newInput();
        }

        return new MemoryInput(delegate.location(), cachedEntry.get());
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        Optional<Slice> cachedEntry = getOrLoadFromCache(key, delegate);
        if (cachedEntry.isEmpty()) {
            largeFileSkippedCount.incrementAndGet();
            return delegate.newStream();
        }

        return new MemoryInputStream(delegate.location(), cachedEntry.get());
    }

    @Override
    public long cacheLength(TrinoInputFile delegate, String key)
            throws IOException
    {
        Optional<Slice> cachedEntry = getOrLoadFromCache(key, delegate);
        if (cachedEntry.isEmpty()) {
            largeFileSkippedCount.incrementAndGet();
            return delegate.length();
        }

        return cachedEntry.get().length();
    }

    @Override
    public void expire(Location location)
            throws IOException
    {
        List<String> expired = cache.asMap().keySet().stream()
                .filter(key -> key.startsWith(location.path()))
                .collect(toImmutableList());
        cache.invalidateAll(expired);
    }

    @Override
    public void expire(Collection<Location> locations)
            throws IOException
    {
        List<String> expired = cache.asMap().keySet().stream()
                .filter(key -> locations.stream().map(Location::path).anyMatch(key::startsWith))
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
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Managed
    public long getLargeFileSkippedCount()
    {
        return largeFileSkippedCount.get();
    }

    @VisibleForTesting
    boolean isCached(String key)
    {
        Optional<Slice> cachedEntry = cache.getIfPresent(key);
        return cachedEntry != null && cachedEntry.isPresent();
    }

    private Optional<Slice> getOrLoadFromCache(String key, TrinoInputFile delegate)
            throws IOException
    {
        try {
            return cache.get(key, () -> load(delegate));
        }
        catch (ExecutionException e) {
            throw handleException(delegate.location(), e.getCause());
        }
    }

    private Optional<Slice> load(TrinoInputFile delegate)
            throws IOException
    {
        long fileSize = delegate.length();
        if (fileSize > maxContentLengthBytes) {
            return Optional.empty();
        }
        try (TrinoInput trinoInput = delegate.newInput()) {
            return Optional.of(trinoInput.readTail(toIntExact(fileSize)));
        }
    }

    private static IOException handleException(Location location, Throwable cause)
            throws IOException
    {
        if (cause instanceof FileNotFoundException || cause instanceof NoSuchFileException) {
            throw withCause(new FileNotFoundException(location.toString()), cause);
        }
        if (cause instanceof FileAlreadyExistsException) {
            throw withCause(new FileAlreadyExistsException(location.toString()), cause);
        }
        throw new IOException(cause.getMessage() + ": " + location, cause);
    }

    private static <T extends Throwable> T withCause(T throwable, Throwable cause)
    {
        throwable.initCause(cause);
        return throwable;
    }
}
