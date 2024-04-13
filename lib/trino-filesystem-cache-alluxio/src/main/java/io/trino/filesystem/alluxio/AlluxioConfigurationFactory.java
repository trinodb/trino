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
package io.trino.filesystem.alluxio;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_DIRS;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_ENABLED;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_SIZE;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_TTL_ENABLED;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_TTL_THRESHOLD_SECONDS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig.CACHE_DIRECTORIES;
import static io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig.CACHE_MAX_PERCENTAGES;
import static io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig.CACHE_MAX_SIZES;
import static java.lang.String.format;
import static java.lang.String.join;

public class AlluxioConfigurationFactory
{
    private AlluxioConfigurationFactory() {}

    public static AlluxioConfiguration create(AlluxioFileSystemCacheConfig config)
    {
        checkArgument(config.getMaxCacheSizes().isEmpty() ^ config.getMaxCacheDiskUsagePercentages().isEmpty(),
                "Either %s or %s must be specified", CACHE_MAX_SIZES, CACHE_MAX_PERCENTAGES);
        int size = config.getMaxCacheSizes().isEmpty() ? config.getMaxCacheDiskUsagePercentages().size() : config.getMaxCacheSizes().size();
        checkArgument(config.getCacheDirectories().size() == size,
                "%s and %s must have the same size", CACHE_DIRECTORIES, config.getMaxCacheSizes().isEmpty() ? CACHE_MAX_PERCENTAGES : CACHE_MAX_SIZES);
        config.getCacheDirectories().forEach(directory -> canWrite(Path.of(directory)));
        List<DataSize> maxCacheSizes = config.getMaxCacheSizes().isEmpty() ?
                calculateMaxCacheSizes(config.getMaxCacheDiskUsagePercentages(), config.getCacheDirectories().stream()
                        .map(directory -> totalSpace(Path.of(directory))).collect(toImmutableList()))
                : config.getMaxCacheSizes();

        AlluxioProperties alluxioProperties = new AlluxioProperties();
        alluxioProperties.set(USER_CLIENT_CACHE_ENABLED, true);
        alluxioProperties.set(USER_CLIENT_CACHE_DIRS, join(",", config.getCacheDirectories()));
        alluxioProperties.set(USER_CLIENT_CACHE_SIZE, join(",", maxCacheSizes.stream().map(DataSize::toBytesValueString).toList()));
        alluxioProperties.set(USER_CLIENT_CACHE_PAGE_SIZE, config.getCachePageSize().toBytesValueString());
        Optional<Duration> ttl = config.getCacheTTL();
        if (ttl.isPresent()) {
            alluxioProperties.set(USER_CLIENT_CACHE_TTL_THRESHOLD_SECONDS, ttl.orElseThrow().roundTo(TimeUnit.SECONDS));
            alluxioProperties.set(USER_CLIENT_CACHE_TTL_ENABLED, true);
        }
        return new InstancedConfiguration(alluxioProperties);
    }

    private static void canWrite(Path path)
    {
        Path originalPath = path;
        while (!Files.exists(path) && path.getParent() != null) {
            path = path.getParent();
        }
        checkArgument(Files.isDirectory(path), format("Cache directory %s is not a directory", path));
        checkArgument(Files.isReadable(path), format("Cannot read from cache directory %s", originalPath));
        checkArgument(Files.isWritable(path), format("Cannot write to cache directory %s", originalPath));
    }

    /**
     * Get total space of the partition named by the path or its parent paths.
     */
    @VisibleForTesting
    static long totalSpace(Path path)
    {
        while (!Files.exists(path) && path.getParent() != null) {
            path = path.getParent();
        }
        return path.toFile().getTotalSpace();
    }

    private static List<DataSize> calculateMaxCacheSizes(List<Integer> cachePercentages, List<Long> cacheDiskSizes)
    {
        ImmutableList.Builder<DataSize> maxCacheSizes = ImmutableList.builderWithExpectedSize(cacheDiskSizes.size());
        for (int i = 0; i < cacheDiskSizes.size(); i++) {
            maxCacheSizes.add(DataSize.of(Math.round(cachePercentages.get(i) / 100.0 * cacheDiskSizes.get(i)), DataSize.Unit.BYTE));
        }
        return maxCacheSizes.build();
    }
}
