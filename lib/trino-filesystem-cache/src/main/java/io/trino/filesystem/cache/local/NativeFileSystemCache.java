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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.filesystem.cache.local.NativeFileSystemCacheConfig.CACHE_DIRECTORIES;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class NativeFileSystemCache
        implements TrinoFileSystemCache
{
    private final Tracer tracer;
    private final List<NativeCacheDirectory> cacheDirectories;
    private final NativeFileSystemCacheStats stats;
    private final int pageSize;

    @Inject
    public NativeFileSystemCache(Tracer tracer, NativeFileSystemCacheConfig config, NativeFileSystemCacheStats stats)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        requireNonNull(config, "config is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.pageSize = toIntExact(config.getCachePageSize().toBytes());
        validate(config);
        this.cacheDirectories = config.getCacheDirectories().stream()
                .map(Path::of)
                .map(path -> createCacheDirectory(path, stats))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    NativeFileSystemCache(List<Path> cacheDirectories, DataSize pageSize, NativeFileSystemCacheStats stats)
    {
        this(TracerProvider.noop().get("native-file-system-cache"), cacheDirectories, pageSize, stats);
    }

    @VisibleForTesting
    NativeFileSystemCache(Tracer tracer, List<Path> cacheDirectories, DataSize pageSize, NativeFileSystemCacheStats stats)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.pageSize = toIntExact(pageSize.toBytes());
        this.cacheDirectories = cacheDirectories.stream()
                .map(path -> createCacheDirectory(path, stats))
                .collect(toImmutableList());
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        return new NativeCacheInput(cacheFile(delegate, key));
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        return new NativeCacheInputStream(cacheFile(delegate, key));
    }

    @Override
    public long cacheLength(TrinoInputFile delegate, String key)
            throws IOException
    {
        return delegate.length();
    }

    @Override
    public void expire(Location location)
            throws IOException
    {
        for (NativeCacheDirectory cacheDirectory : cacheDirectories) {
            cacheDirectory.expire(location);
        }
    }

    @Override
    public void expire(Collection<Location> locations)
            throws IOException
    {
        for (NativeCacheDirectory cacheDirectory : cacheDirectories) {
            cacheDirectory.expire(locations);
        }
    }

    private NativeCacheFile cacheFile(TrinoInputFile delegate, String key)
            throws IOException
    {
        return new NativeCacheFile(tracer, delegate, key, cacheDirectory(delegate.location()), stats, pageSize, delegate.length());
    }

    private NativeCacheDirectory cacheDirectory(Location location)
    {
        int index = Math.floorMod(CacheFileLayout.hash(location.toString()).hashCode(), cacheDirectories.size());
        return cacheDirectories.get(index);
    }

    private static NativeCacheDirectory createCacheDirectory(Path path, NativeFileSystemCacheStats stats)
    {
        try {
            return new NativeCacheDirectory(path, stats);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to initialize cache directory: " + path, e);
        }
    }

    private static void validate(NativeFileSystemCacheConfig config)
    {
        checkArgument(!config.getCacheDirectories().isEmpty(), "%s must be specified", CACHE_DIRECTORIES);
        config.getCacheDirectories().forEach(directory -> canWrite(Path.of(directory)));
    }

    private static void canWrite(Path path)
    {
        Path originalPath = path;
        while (!Files.exists(path) && path.getParent() != null) {
            path = path.getParent();
        }
        checkArgument(Files.isDirectory(path), "Cache directory %s is not a directory", path);
        checkArgument(Files.isReadable(path), "Cannot read from cache directory %s", originalPath);
        checkArgument(Files.isWritable(path), "Cannot write to cache directory %s", originalPath);
    }
}
