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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class AlluxioFileSystemCacheConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    static final String CACHE_DIRECTORIES = "fs.cache.directories";
    static final String CACHE_MAX_SIZES = "fs.cache.max-sizes";
    static final String CACHE_MAX_PERCENTAGES = "fs.cache.max-disk-usage-percentages";

    private List<String> cacheDirectories;
    private List<DataSize> maxCacheSizes = ImmutableList.of();
    private Optional<Duration> cacheTTL = Optional.of(Duration.valueOf("7d"));
    private List<Integer> maxCacheDiskUsagePercentages = ImmutableList.of();
    private DataSize cachePageSize = DataSize.valueOf("1MB");

    @NotNull
    public List<String> getCacheDirectories()
    {
        return cacheDirectories;
    }

    @Config(CACHE_DIRECTORIES)
    @ConfigDescription("Base directory to cache data. Use a comma-separated list to cache data in multiple directories.")
    public AlluxioFileSystemCacheConfig setCacheDirectories(String cacheDirectories)
    {
        this.cacheDirectories = cacheDirectories == null ? null : SPLITTER.splitToList(cacheDirectories);
        return this;
    }

    public List<DataSize> getMaxCacheSizes()
    {
        return maxCacheSizes;
    }

    @Config(CACHE_MAX_SIZES)
    @ConfigDescription("The maximum cache size for a cache directory. Use a comma-separated list of sizes to specify allowed maximum values for each directory.")
    public AlluxioFileSystemCacheConfig setMaxCacheSizes(String maxCacheSizes)
    {
        this.maxCacheSizes = SPLITTER.splitToStream(firstNonNull(maxCacheSizes, "")).map(DataSize::valueOf).collect(toImmutableList());
        return this;
    }

    @NotNull
    public Optional<@MinDuration("0s") Duration> getCacheTTL()
    {
        return cacheTTL;
    }

    @Config("fs.cache.ttl")
    @ConfigDescription("Duration to keep files in the cache prior to eviction")
    public AlluxioFileSystemCacheConfig setCacheTTL(Duration cacheTTL)
    {
        this.cacheTTL = Optional.of(cacheTTL);
        return this;
    }

    public AlluxioFileSystemCacheConfig disableTTL()
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
    public AlluxioFileSystemCacheConfig setMaxCacheDiskUsagePercentages(String maxCacheDiskUsagePercentages)
    {
        this.maxCacheDiskUsagePercentages = SPLITTER.splitToStream(firstNonNull(maxCacheDiskUsagePercentages, ""))
                .map(Integer::valueOf)
                .collect(toImmutableList());
        return this;
    }

    @NotNull
    @MaxDataSize("15MB")
    @MinDataSize("64kB")
    public DataSize getCachePageSize()
    {
        return this.cachePageSize;
    }

    @Config("fs.cache.page-size")
    @ConfigDescription("Page size for cache")
    public AlluxioFileSystemCacheConfig setCachePageSize(DataSize cachePageSize)
    {
        this.cachePageSize = cachePageSize;
        return this;
    }
}
