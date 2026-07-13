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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import jakarta.annotation.PreDestroy;

import java.io.IOException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.Objects.requireNonNull;

public class AlluxioCache
{
    private static final HashFunction HASH = Hashing.murmur3_128();

    private final Tracer tracer;
    private final DataSize pageSize;
    private final CacheManager cacheManager;
    private final AlluxioConfiguration config;

    @Inject
    public AlluxioCache(Tracer tracer, AlluxioCacheConfig config)
            throws IOException
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.config = AlluxioConfigurationFactory.create(requireNonNull(config, "config is null"));
        this.pageSize = config.getCachePageSize();
        this.cacheManager = CacheManager.Factory.create(this.config);
    }

    public Blob get(CacheKey key, BlobSource source, AlluxioCacheStats statistics)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(source, "source is null");
        try {
            URIStatus status = uriStatus(cacheIdentifier(key), source);
            CacheManager tracingCacheManager = new TracingCacheManager(tracer, key.toString(), pageSize, cacheManager);
            return new AlluxioBlob(tracer, source, key.toString(), status, tracingCacheManager, config, statistics);
        }
        catch (Throwable e) {
            // The cache owns the source until a blob is returned, so it must not stay open
            // when this method fails
            closeAllSuppress(e, source);
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Hashes the key components with length framing, so distinct component lists can never
     * produce the same identifier regardless of the characters they contain.
     */
    @VisibleForTesting
    static String cacheIdentifier(CacheKey key)
    {
        Hasher hasher = HASH.newHasher();
        for (String component : key.components()) {
            hasher.putInt(component.length());
            hasher.putUnencodedChars(component);
        }
        return hasher.hash().toString();
    }

    // Invalidation is best effort per the BlobCache contract, and the alluxio page store cannot
    // deliver any of it: page ids are one-way hashes of the full cache key, so a key prefix
    // cannot be mapped back to its pages without an in-memory index over every cached blob,
    // which is unbounded (millions of entries) and would not survive restarts anyway. Stale
    // entries are dropped by TTL and size eviction only, so keys must be versioned (file length,
    // lastModified) when content can be overwritten in place.
    public void tryInvalidate(CacheKey prefix) {}

    @PreDestroy
    public void shutdown()
            throws Exception
    {
        cacheManager.close();
    }

    private static URIStatus uriStatus(String cacheIdentifier, BlobSource source)
            throws IOException
    {
        FileInfo info = new FileInfo()
                .setPath(source.toString())
                .setLength(source.length());
        return new URIStatus(info, CacheContext.defaults().setCacheIdentifier(cacheIdentifier));
    }
}
