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
package io.trino.plugin.hive.orc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import java.util.concurrent.TimeUnit;

public class OrcMetadataCacheConfig
{
    private boolean fileTailCacheEnabled;
    private DataSize fileTailCacheSize = DataSize.of(1, DataSize.Unit.GIGABYTE);
    private Duration fileTailCacheTtlSinceLastAccess = new Duration(4, TimeUnit.HOURS);

    public boolean isFileTailCacheEnabled()
    {
        return fileTailCacheEnabled;
    }

    @Config("hive.orc.file-tail-cache-enabled")
    @ConfigDescription("Enable caching of ORC's file tail")
    public OrcMetadataCacheConfig setFileTailCacheEnabled(boolean fileTailCacheEnabled)
    {
        this.fileTailCacheEnabled = fileTailCacheEnabled;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getFileTailCacheSize()
    {
        return fileTailCacheSize;
    }

    @Config("hive.orc.file-tail-cache-size")
    @ConfigDescription("Size of the orc file tail cache")
    public OrcMetadataCacheConfig setFileTailCacheSize(DataSize fileTailCacheSize)
    {
        this.fileTailCacheSize = fileTailCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getFileTailCacheTtlSinceLastAccess()
    {
        return fileTailCacheTtlSinceLastAccess;
    }

    @Config("hive.orc.file-tail-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for file tail cache entry after last access")
    public OrcMetadataCacheConfig setFileTailCacheTtlSinceLastAccess(Duration fileTailCacheTtlSinceLastAccess)
    {
        this.fileTailCacheTtlSinceLastAccess = fileTailCacheTtlSinceLastAccess;
        return this;
    }
}
