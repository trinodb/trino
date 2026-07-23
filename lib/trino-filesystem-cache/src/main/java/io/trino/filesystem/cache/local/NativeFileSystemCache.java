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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.HOURS;

public class NativeFileSystemCache
        implements TrinoFileSystemCache
{
    private static final Logger log = Logger.get(NativeFileSystemCache.class);

    private final List<NativeCacheDirectory> cacheDirectories;
    private final int pageSize;
    private final NativeFileSystemCacheStats stats;
    private final Tracer tracer;
    private final List<ScheduledExecutorService> maintenanceExecutors;

    @Inject
    public NativeFileSystemCache(NativeFileSystemCacheConfig config, NativeFileSystemCacheStats stats, Tracer tracer)
    {
        this.cacheDirectories = createCacheDirectories(requireNonNull(config, "config is null"), requireNonNull(stats, "stats is null"));
        this.pageSize = toIntExact(config.getCachePageSize().toBytes());
        this.stats = stats;
        this.tracer = tracer;
        this.maintenanceExecutors = IntStream.range(0, cacheDirectories.size())
                .mapToObj(index -> newSingleThreadScheduledExecutor(daemonThreadsNamed("native-file-system-cache-maintenance-" + index)))
                .collect(toImmutableList());
        startMaintenance();
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

    @PreDestroy
    public void shutdown()
    {
        maintenanceExecutors.forEach(ScheduledExecutorService::shutdownNow);
    }

    private void startMaintenance()
    {
        for (int index = 0; index < cacheDirectories.size(); index++) {
            startMaintenance(cacheDirectories.get(index), maintenanceExecutors.get(index));
        }
    }

    private static void startMaintenance(NativeCacheDirectory cacheDirectory, ScheduledExecutorService maintenanceExecutor)
    {
        maintenanceExecutor.execute(() -> {
            try {
                cacheDirectory.initialize();
            }
            catch (RuntimeException | Error e) {
                // This background task runs without a retained Future to report failures.
                log.error(e, "Error initializing native file system cache directory: %s", cacheDirectory);
            }
        });
        maintenanceExecutor.scheduleWithFixedDelay(() -> {
            try {
                cacheDirectory.maintenance();
            }
            catch (RuntimeException | Error e) {
                // scheduleWithFixedDelay stops future executions when a failure escapes.
                log.error(e, "Error maintaining native file system cache directory: %s", cacheDirectory);
            }
        }, 1, 1, HOURS);
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

    @VisibleForTesting
    List<NativeCacheDirectory> getCacheDirectories()
    {
        return cacheDirectories;
    }

    private static List<NativeCacheDirectory> createCacheDirectories(NativeFileSystemCacheConfig config, NativeFileSystemCacheStats stats)
    {
        List<DataSize> maxCacheSizes = maxCacheSizes(config);
        DataSize accessTrackingMemory = DataSize.ofBytes(Math.max(1, config.getAccessTrackingMemory().toBytes() / config.getCacheDirectories().size()));
        return IntStream.range(0, config.getCacheDirectories().size())
                .mapToObj(index -> {
                    Path path = Path.of(config.getCacheDirectories().get(index));
                    try {
                        BloomFilterAccessTracker accessTracker = new BloomFilterAccessTracker(path, config.getAccessHistoryDuration(), config.getAccessBucketDuration(), accessTrackingMemory, stats);
                        return new NativeCacheDirectory(path, maxCacheSizes.get(index).toBytes(), config.getMaxCacheFilesPerDirectory(), config.getCacheTTL(), accessTracker, stats);
                    }
                    catch (IOException e) {
                        throw new IllegalArgumentException("Failed to initialize cache directory: " + path, e);
                    }
                })
                .collect(toImmutableList());
    }

    private static List<DataSize> maxCacheSizes(NativeFileSystemCacheConfig config)
    {
        if (!config.getMaxCacheSizes().isEmpty()) {
            return config.getMaxCacheSizes();
        }
        List<DataSize> sizes = new ArrayList<>(config.getCacheDirectories().size());
        for (int index = 0; index < config.getCacheDirectories().size(); index++) {
            long totalSpace = totalSpace(Path.of(config.getCacheDirectories().get(index)));
            sizes.add(DataSize.ofBytes(Math.round(config.getMaxCacheDiskUsagePercentages().get(index) / 100.0 * totalSpace)));
        }
        return sizes;
    }

    @VisibleForTesting
    static long totalSpace(Path path)
    {
        Path originalPath = path;
        while (!Files.exists(path) && path.getParent() != null) {
            path = path.getParent();
        }
        try {
            return Files.getFileStore(path).getTotalSpace();
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get total space for cache directory: " + originalPath, e);
        }
    }
}
