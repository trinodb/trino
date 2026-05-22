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
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class NativeFileSystemCacheConfig
{
    static final String CACHE_DIRECTORIES = "fs.cache.directories";

    private List<String> cacheDirectories = ImmutableList.of();
    private DataSize cachePageSize = DataSize.valueOf("1MB");

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

    @NotNull
    @MaxDataSize("15MB")
    @MinDataSize("64kB")
    public DataSize getCachePageSize()
    {
        return cachePageSize;
    }

    @Config("fs.cache.page-size")
    @ConfigDescription("Page size for cache")
    public NativeFileSystemCacheConfig setCachePageSize(DataSize cachePageSize)
    {
        this.cachePageSize = cachePageSize;
        return this;
    }
}
