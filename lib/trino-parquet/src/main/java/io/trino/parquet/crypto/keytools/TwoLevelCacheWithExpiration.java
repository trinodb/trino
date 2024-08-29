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
package io.trino.parquet.crypto.keytools;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Concurrent two-level cache with expiration of internal caches according to token lifetime.
 * External cache is per token, internal is per String key.
 *
 * @param <V> Value
 */
class TwoLevelCacheWithExpiration<V>
{
    private final ConcurrentMap<String, ExpiringCacheEntry<ConcurrentMap<String, V>>> cache;
    private volatile long lastCacheCleanupTimestamp;

    TwoLevelCacheWithExpiration()
    {
        this.cache = new ConcurrentHashMap<>();
        this.lastCacheCleanupTimestamp = System.currentTimeMillis();
    }

    ConcurrentMap<String, V> getOrCreateInternalCache(String accessToken, long cacheEntryLifetime)
    {
        ExpiringCacheEntry<ConcurrentMap<String, V>> externalCacheEntry =
                cache.compute(accessToken, (token, cacheEntry) -> {
                    if ((null == cacheEntry) || cacheEntry.isExpired()) {
                        return new ExpiringCacheEntry<>(new ConcurrentHashMap<String, V>(), cacheEntryLifetime);
                    }
                    else {
                        return cacheEntry;
                    }
                });
        return externalCacheEntry.getCachedItem();
    }

    void removeCacheEntriesForToken(String accessToken)
    {
        cache.remove(accessToken);
    }

    void removeCacheEntriesForAllTokens()
    {
        cache.clear();
    }

    public void checkCacheForExpiredTokens(long cacheCleanupPeriod)
    {
        long now = System.currentTimeMillis();

        if (now > (lastCacheCleanupTimestamp + cacheCleanupPeriod)) {
            synchronized (cache) {
                if (now > (lastCacheCleanupTimestamp + cacheCleanupPeriod)) {
                    removeExpiredEntriesFromCache();
                    lastCacheCleanupTimestamp = now + cacheCleanupPeriod;
                }
            }
        }
    }

    public void removeExpiredEntriesFromCache()
    {
        cache.values().removeIf(cacheEntry -> cacheEntry.isExpired());
    }

    public void remove(String accessToken)
    {
        cache.remove(accessToken);
    }

    public void clear()
    {
        cache.clear();
    }

    static class ExpiringCacheEntry<E>
    {
        private final long expirationTimestamp;
        private final E cachedItem;

        private ExpiringCacheEntry(E cachedItem, long expirationIntervalMillis)
        {
            this.expirationTimestamp = System.currentTimeMillis() + expirationIntervalMillis;
            this.cachedItem = cachedItem;
        }

        private boolean isExpired()
        {
            final long now = System.currentTimeMillis();
            return (now > expirationTimestamp);
        }

        private E getCachedItem()
        {
            return cachedItem;
        }
    }
}
