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

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

public class NativeFileSystemCacheConfig
{
    private static final String CACHE_DIRECTORIES = "fs.cache.directories";
    private static final String CACHE_MAX_SIZES = "fs.cache.max-sizes";
    private static final String CACHE_MAX_PERCENTAGES = "fs.cache.max-disk-usage-percentages";

    private List<String> cacheDirectories = ImmutableList.of();
    private List<DataSize> maxCacheSizes = ImmutableList.of();
    private Optional<Duration> cacheTTL = Optional.of(Duration.valueOf("7d"));
    private List<Integer> maxCacheDiskUsagePercentages = ImmutableList.of();
    private DataSize cachePageSize = DataSize.valueOf("1MB");
    private long maxCacheFilesPerDirectory = 10_000_000;
    private Duration accessHistoryDuration = Duration.valueOf("24h");
    private Duration accessBucketDuration = Duration.valueOf("10m");
    private DataSize accessTrackingMemory = DataSize.valueOf("256MB");

    @NotNull
    public List<String> getCacheDirectories()
    {
        return cacheDirectories;
    }

    @Config(CACHE_DIRECTORIES)
    @ConfigDescription("Base directory to cache data. Use a comma-separated list to cache data in multiple directories.")
    public NativeFileSystemCacheConfig setCacheDirectories(List<String> cacheDirectories)
    {
        this.cacheDirectories = ImmutableList.copyOf(cacheDirectories);
        return this;
    }

    public List<DataSize> getMaxCacheSizes()
    {
        return maxCacheSizes;
    }

    @Config(CACHE_MAX_SIZES)
    @ConfigDescription("The maximum cache size for a cache directory. Use a comma-separated list of sizes to specify allowed maximum values for each directory.")
    public NativeFileSystemCacheConfig setMaxCacheSizes(List<DataSize> maxCacheSizes)
    {
        this.maxCacheSizes = ImmutableList.copyOf(maxCacheSizes);
        return this;
    }

    @NotNull
    public Optional<@MinDuration("0s") Duration> getCacheTTL()
    {
        return cacheTTL;
    }

    @Config("fs.cache.ttl")
    @ConfigDescription("Maximum duration cached files are kept before they are eligible for best-effort eviction")
    public NativeFileSystemCacheConfig setCacheTTL(Duration cacheTTL)
    {
        this.cacheTTL = Optional.of(cacheTTL);
        return this;
    }

    public NativeFileSystemCacheConfig disableTTL()
    {
        this.cacheTTL = Optional.empty();
        return this;
    }

    public List<@Min(0) @Max(100) Integer> getMaxCacheDiskUsagePercentages()
    {
        return maxCacheDiskUsagePercentages;
    }

    @Config(CACHE_MAX_PERCENTAGES)
    @ConfigDescription("The maximum percentage (0-100) of total disk size the cache can use. Use a comma-separated list of percentage values if supplying several cache directories.")
    public NativeFileSystemCacheConfig setMaxCacheDiskUsagePercentages(List<Integer> maxCacheDiskUsagePercentages)
    {
        this.maxCacheDiskUsagePercentages = ImmutableList.copyOf(maxCacheDiskUsagePercentages);
        return this;
    }

    @NotNull
    @MaxDataSize("15MB")
    @MinDataSize("64kB")
    public DataSize getCachePageSize()
    {
        return cachePageSize;
    }

    @Config("fs.cache.page-size")
    @ConfigDescription("Size of cached file chunks")
    public NativeFileSystemCacheConfig setCachePageSize(DataSize cachePageSize)
    {
        this.cachePageSize = cachePageSize;
        return this;
    }

    @Min(1)
    public long getMaxCacheFilesPerDirectory()
    {
        return maxCacheFilesPerDirectory;
    }

    @Config("fs.cache.max-files-per-directory")
    @ConfigDescription("Maximum number of cached chunk files in each cache directory")
    public NativeFileSystemCacheConfig setMaxCacheFilesPerDirectory(long maxCacheFilesPerDirectory)
    {
        this.maxCacheFilesPerDirectory = maxCacheFilesPerDirectory;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getAccessHistoryDuration()
    {
        return accessHistoryDuration;
    }

    @Config("fs.cache.access-history-duration")
    @ConfigDescription("Duration of approximate recent cache access history to keep for eviction")
    public NativeFileSystemCacheConfig setAccessHistoryDuration(Duration accessHistoryDuration)
    {
        this.accessHistoryDuration = accessHistoryDuration;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getAccessBucketDuration()
    {
        return accessBucketDuration;
    }

    @Config("fs.cache.access-bucket-duration")
    @ConfigDescription("Duration of each approximate access tracking bucket")
    public NativeFileSystemCacheConfig setAccessBucketDuration(Duration accessBucketDuration)
    {
        this.accessBucketDuration = accessBucketDuration;
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    public DataSize getAccessTrackingMemory()
    {
        return accessTrackingMemory;
    }

    @Config("fs.cache.access-tracking-memory")
    @ConfigDescription("Maximum memory used for approximate recent access tracking")
    public NativeFileSystemCacheConfig setAccessTrackingMemory(DataSize accessTrackingMemory)
    {
        this.accessTrackingMemory = accessTrackingMemory;
        return this;
    }

    @AssertTrue(message = CACHE_DIRECTORIES + " must be specified")
    public boolean isCacheDirectoriesConfigured()
    {
        return !cacheDirectories.isEmpty();
    }

    @AssertTrue(message = "Either " + CACHE_MAX_SIZES + " or " + CACHE_MAX_PERCENTAGES + " must be specified")
    public boolean isCacheMaxSizeConfigured()
    {
        return maxCacheSizes.isEmpty() ^ maxCacheDiskUsagePercentages.isEmpty();
    }

    @AssertTrue(message = CACHE_DIRECTORIES + " and configured cache size limits must have the same size")
    public boolean isCacheMaxSizeCountValid()
    {
        if (!isCacheMaxSizeConfigured()) {
            return true;
        }
        int size = maxCacheSizes.isEmpty() ? maxCacheDiskUsagePercentages.size() : maxCacheSizes.size();
        return cacheDirectories.size() == size;
    }

    @AssertTrue(message = "fs.cache.access-history-duration must be greater than or equal to fs.cache.access-bucket-duration")
    public boolean isAccessDurationValid()
    {
        return accessHistoryDuration.toMillis() >= accessBucketDuration.toMillis();
    }
}
