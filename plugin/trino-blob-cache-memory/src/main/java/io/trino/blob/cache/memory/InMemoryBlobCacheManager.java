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
import io.airlift.log.Logger;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheCapability;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.catalog.CatalogName;
import org.weakref.jmx.MBeanExporter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.cache.CacheCapability.LOW_LATENCY;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;

public class InMemoryBlobCacheManager
        implements BlobCacheManager
{
    private static final Logger log = Logger.get(InMemoryBlobCacheManager.class);

    private final MemoryBlobCache sharedCache;
    private final MBeanExporter exporter;
    private final Map<CatalogName, CatalogEntry> catalogs = new ConcurrentHashMap<>();

    @Inject
    public InMemoryBlobCacheManager(MemoryBlobCache sharedCache, MBeanExporter exporter)
    {
        this.sharedCache = requireNonNull(sharedCache, "sharedCache is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
    }

    @Override
    public boolean hasCapability(CacheCapability capability)
    {
        return capability == LOW_LATENCY;
    }

    @Override
    public BlobCache create(CatalogName catalog, Set<CacheCapability> capabilities)
    {
        // keys arrive with the catalog name as their first component, so catalogs can share the cache
        requireNonNull(catalog, "catalog is null");
        return catalogs.computeIfAbsent(catalog, _ -> {
            MemoryBlobCacheStats stats = new MemoryBlobCacheStats();
            String name = generatedNameOf(MemoryBlobCacheStats.class, catalog.toString());
            CatalogEntry entry = new CatalogEntry(name, new StatsRecordingBlobCache(sharedCache, stats));
            try {
                exporter.export(name, stats);
            }
            catch (Exception e) {
                log.warn(e, "Failed to register MemoryBlobCacheStats MBean for catalog %s", catalog);
            }
            return entry;
        }).blobCache();
    }

    @Override
    public void drop(CatalogName catalog)
    {
        CatalogEntry entry = catalogs.remove(catalog);
        if (entry != null) {
            try {
                exporter.unexport(entry.objectName());
            }
            catch (Exception e) {
                log.warn(e, "Failed to unregister MemoryBlobCacheStats MBean for catalog %s", catalog);
            }
        }
        sharedCache.tryInvalidate(CacheKey.of(catalog.toString()));
    }

    @Override
    public void shutdown()
    {
        Set.copyOf(catalogs.keySet()).forEach(this::drop);
        sharedCache.flushCache();
    }

    private record CatalogEntry(String objectName, BlobCache blobCache) {}

    private record StatsRecordingBlobCache(MemoryBlobCache delegate, MemoryBlobCacheStats stats)
            implements BlobCache
    {
        @Override
        public Blob get(CacheKey key, BlobSource source)
                throws IOException
        {
            return delegate.get(key, source, stats);
        }

        @Override
        public void tryInvalidate(CacheKey prefix)
        {
            delegate.tryInvalidate(prefix);
        }
    }
}
