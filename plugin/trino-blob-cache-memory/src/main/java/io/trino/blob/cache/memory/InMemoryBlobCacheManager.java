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
import io.trino.spi.cache.CacheKey;
import io.trino.spi.catalog.CatalogName;

import java.io.IOException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class InMemoryBlobCacheManager
        implements BlobCacheManager
{
    private final MemoryBlobCache sharedCache;

    @Inject
    public InMemoryBlobCacheManager(MemoryBlobCache sharedCache)
    {
        this.sharedCache = requireNonNull(sharedCache, "sharedCache is null");
    }

    @Override
    public BlobCache createBlobCache(CatalogName catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return new CatalogScopedBlobCache(sharedCache, prefix(catalog));
    }

    @Override
    public void drop(CatalogName catalog)
    {
        sharedCache.invalidatePrefix(prefix(catalog));
    }

    private static String prefix(CatalogName catalog)
    {
        String name = catalog.toString();
        checkArgument(!name.contains("\0"), "catalog name contains NUL byte: %s", name);
        return name + "\0";
    }

    @Override
    public void shutdown()
    {
        sharedCache.flushCache();
    }

    private record CatalogScopedBlobCache(MemoryBlobCache delegate, String prefix)
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
