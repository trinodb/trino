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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.base.group.CachingGroupProviderModule.ForCachingGroupProvider;
import io.trino.spi.security.GroupProvider;

import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingGroupProvider
        implements GroupProvider, GroupCacheInvalidationController
{
    private final LoadingCache<String, Set<String>> cache;

    @Inject
    public CachingGroupProvider(CachingGroupProviderConfig config, @ForCachingGroupProvider GroupProvider delegate)
    {
        requireNonNull(delegate, "delegate is null");
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumSize(config.getCacheMaximumSize())
                .expireAfterWrite(config.getTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build(CacheLoader.from(delegate::getGroups));
    }

    @Override
    public Set<String> getGroups(String user)
    {
        return cache.getUnchecked(user);
    }

    @Override
    public void invalidate(String user)
    {
        cache.invalidate(user);
    }

    @Override
    public void invalidateAll()
    {
        cache.invalidateAll();
    }
}
