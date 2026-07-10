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
package io.trino.plugin.metastore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import static java.util.concurrent.TimeUnit.HOURS;

public class HetuMetastoreCacheConfig
{
    private long metaStoreCacheMaxSize = 10000;
    private Duration metaStoreCacheTtl = new Duration(4, HOURS);

    @Config("trino.metastore.cache.size")
    @ConfigDescription("Set the max metastore cache size, default value 50000.")
    public HetuMetastoreCacheConfig setMetaStoreCacheMaxSize(long metaStoreCacheMaxSize)
    {
        this.metaStoreCacheMaxSize = metaStoreCacheMaxSize;
        return this;
    }

    public long getMetaStoreCacheMaxSize()
    {
        return metaStoreCacheMaxSize;
    }

    @Config("trino.metastore.cache.ttl")
    @ConfigDescription("Set ttl for metastore cache, default value 4h.")
    public HetuMetastoreCacheConfig setMetaStoreCacheTtl(Duration metaStoreCacheTtl)
    {
        this.metaStoreCacheTtl = metaStoreCacheTtl;
        return this;
    }

    @MinDuration("0ms")
    public Duration getMetaStoreCacheTtl()
    {
        return metaStoreCacheTtl;
    }
}
