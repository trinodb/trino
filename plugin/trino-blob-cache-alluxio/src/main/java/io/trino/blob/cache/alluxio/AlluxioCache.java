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

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.FileInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.util.Collection;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AlluxioCache
{
    private final DataSize pageSize;
    private final CacheManager cacheManager;
    private final AlluxioConfiguration config;
    private final HashFunction hashFunction = Hashing.murmur3_128();

    @Inject
    public AlluxioCache(AlluxioCacheConfig config)
            throws IOException
    {
        this.config = AlluxioConfigurationFactory.create(requireNonNull(config, "config is null"));
        this.pageSize = config.getCachePageSize();
        this.cacheManager = CacheManager.Factory.create(this.config);
    }

    public Blob get(CacheKey key, BlobSource source, Tracer tracer, AlluxioCacheStats statistics)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(source, "source is null");
        URIStatus status = uriStatus(key, source);
        CacheManager tracingCacheManager = new TracingCacheManager(tracer, key.key(), pageSize, cacheManager);
        return new AlluxioBlob(tracer, source, key.key(), status, tracingCacheManager, config, statistics);
    }

    // TODO: explicit invalidation is not implemented; entries are evicted by TTL and size limits.
    // Implementing this requires mapping a CacheKey back to the Alluxio PageIds stored for the blob
    // (see AlluxioBlobSource.pageId) and invoking CacheManager.delete for each, which needs the
    // source length that is not known at invalidation time.
    public void invalidate(CacheKey key) {}

    public void invalidate(Collection<CacheKey> keys) {}

    @PreDestroy
    public void shutdown()
            throws Exception
    {
        cacheManager.close();
    }

    @VisibleForTesting
    URIStatus uriStatus(CacheKey key, BlobSource source)
            throws IOException
    {
        FileInfo info = new FileInfo()
                .setPath(source.toString())
                .setLength(source.length());
        String cacheIdentifier = hashFunction.hashString(key.key(), UTF_8).toString();
        return new URIStatus(info, CacheContext.defaults().setCacheIdentifier(cacheIdentifier));
    }
}
