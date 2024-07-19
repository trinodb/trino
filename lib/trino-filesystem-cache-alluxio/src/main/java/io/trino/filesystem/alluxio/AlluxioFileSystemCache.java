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
package io.trino.filesystem.alluxio;

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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.util.Collection;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AlluxioFileSystemCache
        implements TrinoFileSystemCache
{
    private final Tracer tracer;
    private final DataSize pageSize;
    private final CacheManager cacheManager;
    private final AlluxioConfiguration config;
    private final AlluxioCacheStats statistics;
    private final HashFunction hashFunction = Hashing.murmur3_128();

    @Inject
    public AlluxioFileSystemCache(Tracer tracer, AlluxioFileSystemCacheConfig config, AlluxioCacheStats statistics)
            throws IOException
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.config = AlluxioConfigurationFactory.create(requireNonNull(config, "config is null"));
        this.pageSize = config.getCachePageSize();
        this.cacheManager = CacheManager.Factory.create(this.config);
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        return new AlluxioInput(tracer, delegate, key, uriStatus(delegate, key), new TracingCacheManager(tracer, key, pageSize, cacheManager), config, statistics);
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        return new AlluxioInputStream(tracer, delegate, key, uriStatus(delegate, key), new TracingCacheManager(tracer, key, pageSize, cacheManager), config, statistics);
    }

    @Override
    public long cacheLength(TrinoInputFile delegate, String key)
            throws IOException
    {
        return delegate.length();
    }

    @Override
    public void expire(Location source)
            throws IOException
    {
    }

    @Override
    public void expire(Collection<Location> locations)
            throws IOException
    {
    }

    @PreDestroy
    public void shutdown()
            throws Exception
    {
        cacheManager.close();
    }

    @VisibleForTesting
    protected URIStatus uriStatus(TrinoInputFile file, String key)
            throws IOException
    {
        FileInfo info = new FileInfo()
                .setPath(file.location().toString())
                .setLength(file.length());
        String cacheIdentifier = hashFunction.hashString(key, UTF_8).toString();
        return new URIStatus(info, CacheContext.defaults().setCacheIdentifier(cacheIdentifier));
    }
}
