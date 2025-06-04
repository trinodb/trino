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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import jakarta.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.HOURS;

public class MemoryFileSystemCacheConfig
{
    // Runtime.getRuntime().maxMemory() is not 100% stable and may return slightly different value over JVM lifetime. We use
    // constant so default configuration for cache size is stable.
    @VisibleForTesting
    static final DataSize DEFAULT_CACHE_SIZE = DataSize.succinctBytes(Math.min(
            Math.floorDiv(Runtime.getRuntime().maxMemory(), 20L),
            DataSize.of(200, MEGABYTE).toBytes()));

    private Duration ttl = new Duration(1, HOURS);
    private DataSize maxSize = DEFAULT_CACHE_SIZE;
    private DataSize maxContentLength = DataSize.of(8, MEGABYTE);

    @NotNull
    public Duration getCacheTtl()
    {
        return ttl;
    }

    @Config("fs.memory-cache.ttl")
    @ConfigDescription("Duration to keep files in the cache prior to eviction")
    public MemoryFileSystemCacheConfig setCacheTtl(Duration ttl)
    {
        this.ttl = ttl;
        return this;
    }

    @NotNull
    public DataSize getMaxSize()
    {
        return maxSize;
    }

    @Config("fs.memory-cache.max-size")
    @ConfigDescription("Maximum total size of the cache")
    public MemoryFileSystemCacheConfig setMaxSize(DataSize maxSize)
    {
        this.maxSize = maxSize;
        return this;
    }

    @NotNull
    // Avoids humongous allocations with the recommended G1HeapRegionSize of 32MB and prevents a few big files from hogging cache space
    @MaxDataSize("15MB")
    public DataSize getMaxContentLength()
    {
        return maxContentLength;
    }

    @Config("fs.memory-cache.max-content-length")
    @ConfigDescription("Maximum size of file that can be cached")
    public MemoryFileSystemCacheConfig setMaxContentLength(DataSize maxContentLength)
    {
        this.maxContentLength = maxContentLength;
        return this;
    }
}
