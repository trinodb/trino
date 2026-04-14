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
package io.trino.blob.cache.memory;

import com.google.inject.Inject;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheInfo;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.CacheTier;
import io.trino.spi.catalog.CatalogName;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class InMemoryBlobCacheManager
        implements BlobCacheManager
{
    private final InMemoryBlobCache sharedCache;
    private final Set<CatalogName> catalogs = ConcurrentHashMap.newKeySet();

    @Inject
    public InMemoryBlobCacheManager(InMemoryBlobCache sharedCache)
    {
        this.sharedCache = requireNonNull(sharedCache, "sharedCache is null");
    }

    @Override
    public BlobCache createBlobCache(CatalogName catalog, Duration ttl)
    {
        requireNonNull(catalog, "catalog is null");
        catalogs.add(catalog);
        return new CatalogScopedBlobCache(sharedCache, catalog.toString() + "\0");
    }

    @Override
    public void invalidate(CatalogName catalog)
    {
        sharedCache.invalidatePrefix(catalog.toString() + "\0");
    }

    @Override
    public void drop(CatalogName catalog)
    {
        invalidate(catalog);
        catalogs.remove(catalog);
    }

    @Override
    public Collection<CacheInfo> getCaches()
    {
        return catalogs.stream()
                .map(catalog -> new CacheInfo(
                        catalog,
                        "memory",
                        CacheTier.MEMORY,
                        sharedCache.size(),
                        sharedCache.getHitCount(),
                        sharedCache.getMissCount(),
                        sharedCache.evictionCount()))
                .collect(toImmutableList());
    }

    @Override
    public void shutdown()
    {
        sharedCache.flushCache();
        catalogs.clear();
    }

    private record CatalogScopedBlobCache(InMemoryBlobCache delegate, String prefix)
            implements BlobCache
    {
        @Override
        public Blob get(CacheKey key, BlobSource source)
                throws IOException
        {
            return delegate.get(new CacheKey(prefix + key.key()), source);
        }

        @Override
        public void invalidate(CacheKey key)
        {
            delegate.invalidate(new CacheKey(prefix + key.key()));
        }

        @Override
        public void invalidate(Collection<CacheKey> keys)
        {
            delegate.invalidate(keys.stream().map(k -> new CacheKey(prefix + k.key())).collect(toImmutableList()));
        }
    }
}
