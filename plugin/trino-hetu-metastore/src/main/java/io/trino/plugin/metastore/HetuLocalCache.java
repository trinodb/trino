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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.log.Logger;
import io.trino.spi.metastore.HetuCache;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class HetuLocalCache<K, V>
        implements HetuCache<K, V>
{
    private static final Logger LOG = Logger.get(HetuLocalCache.class);
    private final Cache<K, V> localCache;

    public HetuLocalCache(HetuMetastoreCacheConfig hetuMetastoreCacheConfig)
    {
        this.localCache = CacheBuilder.newBuilder().maximumSize(hetuMetastoreCacheConfig.getMetaStoreCacheMaxSize())
                .expireAfterAccess(Duration.ofMillis(hetuMetastoreCacheConfig.getMetaStoreCacheTtl().toMillis()))
                .build();
    }

    @Override
    public void invalidate(K key)
    {
        localCache.invalidate(key);
    }

    @Override
    public void invalidateAll()
    {
        localCache.invalidateAll();
    }

    @Override
    public V getIfAbsent(K key, Callable<? extends V> loader)
    {
        try {
            return localCache.get(key, loader);
        }
        catch (ExecutionException e) {
            LOG.info("Error message: " + e.getStackTrace());
        }
        return null;
    }

    @Override
    public V getIfPresent(K key)
    {
        return localCache.getIfPresent(key);
    }
}
