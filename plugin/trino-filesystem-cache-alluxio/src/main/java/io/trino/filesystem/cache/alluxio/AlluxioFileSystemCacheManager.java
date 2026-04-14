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
package io.trino.filesystem.cache.alluxio;

import com.google.inject.Inject;
import io.trino.spi.cache.CacheInfo;
import io.trino.spi.cache.CacheTier;
import io.trino.spi.cache.FileSystemCacheManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.filesystem.Location;
import io.trino.spi.filesystem.TrinoInput;
import io.trino.spi.filesystem.TrinoInputFile;
import io.trino.spi.filesystem.TrinoInputStream;
import io.trino.spi.filesystem.cache.ConnectorFileSystemCache;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AlluxioFileSystemCacheManager
        implements FileSystemCacheManager
{
    private final AlluxioFileSystemCache sharedCache;
    private final Set<CatalogName> catalogs = ConcurrentHashMap.newKeySet();

    @Inject
    public AlluxioFileSystemCacheManager(AlluxioFileSystemCache sharedCache)
    {
        this.sharedCache = requireNonNull(sharedCache, "sharedCache is null");
    }

    @Override
    public ConnectorFileSystemCache createFileSystemCache(CatalogName catalog, Duration ttl)
    {
        requireNonNull(catalog, "catalog is null");
        catalogs.add(catalog);
        return new CatalogScopedConnectorFileSystemCache(sharedCache, catalog);
    }

    @Override
    public void invalidate(CatalogName catalog)
    {
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
                        "alluxio",
                        CacheTier.DISK,
                        0L,
                        0L,
                        0L,
                        0L))
                .collect(toImmutableList());
    }

    @Override
    public void shutdown()
    {
        try {
            sharedCache.shutdown();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        catalogs.clear();
    }

    private record CatalogScopedConnectorFileSystemCache(AlluxioFileSystemCache delegate, CatalogName catalogName)
            implements ConnectorFileSystemCache
    {
        @Override
        public TrinoInput cacheInput(TrinoInputFile file, String key)
                throws IOException
        {
            return delegate.cacheInput(file, cacheKey(catalogName, key));
        }

        @Override
        public TrinoInputStream cacheStream(TrinoInputFile file, String key)
                throws IOException
        {
            return delegate.cacheStream(file, cacheKey(catalogName, key));
        }

        @Override
        public long cacheLength(TrinoInputFile file, String key)
                throws IOException
        {
            return delegate.cacheLength(file, cacheKey(catalogName, key));
        }

        @Override
        public void expire(Location location)
                throws IOException
        {
            delegate.expire(location);
        }

        @Override
        public void expire(Collection<Location> locations)
                throws IOException
        {
            delegate.expire(locations);
        }

        private static String cacheKey(CatalogName catalogName, String key)
        {
            return "catalog:" + catalogName.toString() + ",key:" + key;
        }
    }
}
