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
package io.trino.metastore.cache;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class ImpersonationCachingConfig
{
    private Duration userMetastoreCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private long userMetastoreCacheMaximumSize = 1000;

    @NotNull
    public Duration getUserMetastoreCacheTtl()
    {
        return userMetastoreCacheTtl;
    }

    @Config("hive.user-metastore-cache-ttl")
    public ImpersonationCachingConfig setUserMetastoreCacheTtl(Duration userMetastoreCacheTtl)
    {
        this.userMetastoreCacheTtl = userMetastoreCacheTtl;
        return this;
    }

    @Min(0)
    public long getUserMetastoreCacheMaximumSize()
    {
        return userMetastoreCacheMaximumSize;
    }

    @Config("hive.user-metastore-cache-maximum-size")
    public ImpersonationCachingConfig setUserMetastoreCacheMaximumSize(long userMetastoreCacheMaximumSize)
    {
        this.userMetastoreCacheMaximumSize = userMetastoreCacheMaximumSize;
        return this;
    }
}
