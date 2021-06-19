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
package io.trino.plugin.hive.metastore.cache;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class CachingHiveMetastoreConfig
{
    private Duration metastoreCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private Optional<Duration> metastoreRefreshInterval = Optional.empty();
    private long metastoreCacheMaximumSize = 10000;
    private int maxMetastoreRefreshThreads = 10;

    @NotNull
    public Duration getMetastoreCacheTtl()
    {
        return metastoreCacheTtl;
    }

    @Config("hive.metastore-cache-ttl")
    public CachingHiveMetastoreConfig setMetastoreCacheTtl(Duration metastoreCacheTtl)
    {
        this.metastoreCacheTtl = metastoreCacheTtl;
        return this;
    }

    @NotNull
    public Optional<@MinDuration("1ms") Duration> getMetastoreRefreshInterval()
    {
        return metastoreRefreshInterval;
    }

    @Config("hive.metastore-refresh-interval")
    public CachingHiveMetastoreConfig setMetastoreRefreshInterval(Duration metastoreRefreshInterval)
    {
        this.metastoreRefreshInterval = Optional.ofNullable(metastoreRefreshInterval);
        return this;
    }

    @Min(1)
    public long getMetastoreCacheMaximumSize()
    {
        return metastoreCacheMaximumSize;
    }

    @Config("hive.metastore-cache-maximum-size")
    public CachingHiveMetastoreConfig setMetastoreCacheMaximumSize(long metastoreCacheMaximumSize)
    {
        this.metastoreCacheMaximumSize = metastoreCacheMaximumSize;
        return this;
    }

    @Min(1)
    public int getMaxMetastoreRefreshThreads()
    {
        return maxMetastoreRefreshThreads;
    }

    @Config("hive.metastore-refresh-max-threads")
    public CachingHiveMetastoreConfig setMaxMetastoreRefreshThreads(int maxMetastoreRefreshThreads)
    {
        this.maxMetastoreRefreshThreads = maxMetastoreRefreshThreads;
        return this;
    }
}
