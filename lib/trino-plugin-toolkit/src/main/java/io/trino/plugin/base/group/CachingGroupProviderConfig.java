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
package io.trino.plugin.base.group;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingGroupProviderConfig
{
    private Duration ttl = new Duration(5, SECONDS);
    private long cacheMaximumSize = Long.MAX_VALUE;

    public Duration getTtl()
    {
        return ttl;
    }

    @Config("cache.ttl")
    @ConfigDescription("Determines how long group information will be cached for each user")
    public CachingGroupProviderConfig setTtl(Duration ttl)
    {
        this.ttl = requireNonNull(ttl, "ttl is null");
        return this;
    }

    @Min(1)
    public long getCacheMaximumSize()
    {
        return cacheMaximumSize;
    }

    @Config("cache.maximum-size")
    @ConfigDescription("Maximum number of users for which groups are stored in the cache")
    public CachingGroupProviderConfig setCacheMaximumSize(long cacheMaximumSize)
    {
        this.cacheMaximumSize = cacheMaximumSize;
        return this;
    }
}
