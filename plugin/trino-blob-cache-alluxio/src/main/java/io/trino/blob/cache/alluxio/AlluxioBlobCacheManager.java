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
package io.trino.blob.cache.alluxio;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheInfo;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.catalog.CatalogName;
import org.weakref.jmx.MBeanExporter;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.cache.CacheTier.DISK;
import static java.util.Objects.requireNonNull;

public class AlluxioBlobCacheManager
        implements BlobCacheManager
{
    private static final Logger log = Logger.get(AlluxioBlobCacheManager.class);

    private final Tracer tracer;
    private final AlluxioBlobCache sharedCache;
    private final MBeanExporter exporter;
    private final Map<CatalogName, CatalogEntry> catalogs = new ConcurrentHashMap<>();

    @Inject
    public AlluxioBlobCacheManager(Tracer tracer, AlluxioBlobCache sharedCache, MBeanExporter exporter)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.sharedCache = requireNonNull(sharedCache, "sharedCache is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
    }

    @Override
    public BlobCache createBlobCache(CatalogName catalog, Duration ttl)
    {
        requireNonNull(catalog, "catalog is null");
        return catalogs.computeIfAbsent(catalog, c -> {
            AlluxioCacheStats stats = new AlluxioCacheStats();
            ObjectName name = statsObjectName(catalog);
            exporter.export(name.getCanonicalName(), stats);
            return new CatalogEntry(stats, name, new CatalogScopedBlobCache(sharedCache, catalog, tracer, stats));
        }).blobCache();
    }

    @Override
    public void invalidate(CatalogName catalog) {}

    @Override
    public void drop(CatalogName catalog)
    {
        CatalogEntry entry = catalogs.remove(catalog);
        if (entry != null) {
            try {
                exporter.unexport(entry.objectName().getCanonicalName());
            }
            catch (Exception e) {
                log.warn(e, "Failed to unregister AlluxioCacheStats MBean for catalog %s", catalog);
            }
        }
    }

    @Override
    public Collection<CacheInfo> getCaches()
    {
        return catalogs.keySet().stream()
                .map(catalog -> new CacheInfo(catalog, "alluxio", DISK, 0L, 0L, 0L, 0L))
                .collect(toImmutableList());
    }

    @Override
    public void shutdown()
    {
        for (Map.Entry<CatalogName, CatalogEntry> entry : catalogs.entrySet()) {
            try {
                exporter.unexport(entry.getValue().objectName().getCanonicalName());
            }
            catch (Exception e) {
                log.warn(e, "Failed to unregister AlluxioCacheStats MBean for catalog %s", entry.getKey());
            }
        }
        catalogs.clear();
        try {
            sharedCache.shutdown();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ObjectName statsObjectName(CatalogName catalog)
    {
        try {
            return ObjectName.getInstance(
                    "io.trino.blob.cache.alluxio:catalog=" + catalog + ",name=" + catalog + ",type=" + AlluxioCacheStats.class.getSimpleName());
        }
        catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException("Invalid catalog name: " + catalog, e);
        }
    }

    private record CatalogEntry(AlluxioCacheStats stats, ObjectName objectName, BlobCache blobCache) {}

    private record CatalogScopedBlobCache(AlluxioBlobCache delegate, CatalogName catalog, Tracer tracer, AlluxioCacheStats stats)
            implements BlobCache
    {
        @Override
        public Blob get(CacheKey key, BlobSource source)
                throws IOException
        {
            return delegate.get(scopedKey(key), source, tracer, stats);
        }

        @Override
        public void invalidate(CacheKey key)
        {
            delegate.invalidate(scopedKey(key));
        }

        @Override
        public void invalidate(Collection<CacheKey> keys)
        {
            delegate.invalidate(keys.stream()
                    .map(this::scopedKey)
                    .collect(toImmutableList()));
        }

        private CacheKey scopedKey(CacheKey key)
        {
            return new CacheKey("catalog:" + catalog + ",key:" + key.key());
        }
    }
}
