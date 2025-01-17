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

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.wire.FileInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
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

import static java.util.Objects.requireNonNull;

public class RemoteAlluxioFileSystemCache
        implements TrinoFileSystemCache
{
    private static final Logger log = Logger.get(TrinoFileSystemCache.class);
    private final Tracer tracer;
    private final FileSystem fileSystem;
    private final AlluxioConfiguration config;
    private final AlluxioCacheStats statistics;
    private final CacheManager cacheManager;
    private final DataSize pageSize;

    @Inject
    public RemoteAlluxioFileSystemCache(Tracer tracer, AlluxioFileSystemCacheConfig config, AlluxioCacheStats statistics)
            throws IOException
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.config = AlluxioConfigurationFactory.create(requireNonNull(config, "config is null"));
        this.pageSize = config.getCachePageSize();
        this.cacheManager = CacheManager.Factory.create(this.config);
        FileSystemContext fsContext = FileSystemContext.create(this.config);
        this.fileSystem = FileSystem.Factory.create(fsContext);
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        return new RemoteAlluxioInput(tracer, delegate, key, uriStatus(delegate), new TracingCacheManager(tracer, key, pageSize, cacheManager), config, statistics, openFile(delegate));
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        return new RemoteAlluxioInputStream(tracer, delegate, key, uriStatus(delegate), new TracingCacheManager(tracer, key, pageSize, cacheManager), config, statistics, openFile(delegate));
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
        try {
            AlluxioURI sourceUri = new AlluxioURI(source.toString());
            if (fileSystem.exists(sourceUri) && !sourceUri.isRoot()) {
                fileSystem.free(sourceUri);
            }
        }
        catch (AlluxioException e) {
            log.error(e, "Failed to free the file %s", source);
        }
    }

    @Override
    public void expire(Collection<Location> locations)
            throws IOException
    {
        for (Location location : locations) {
            try {
                AlluxioURI locationUri = new AlluxioURI(location.toString());
                if (fileSystem.exists(locationUri) && !locationUri.isRoot()) {
                    fileSystem.free(locationUri);
                }
            }
            catch (AlluxioException e) {
                log.error(e, "Failed to free the file %s", location);
            }
        }
    }

    @PreDestroy
    public void shutdown()
            throws Exception
    {
        cacheManager.close();
        fileSystem.close();
    }

    @VisibleForTesting
    protected URIStatus uriStatus(TrinoInputFile file)
            throws IOException
    {
        FileInfo info = new FileInfo()
                .setPath(file.location().toString())
                .setLength(file.length());
        return new URIStatus(info);
    }

    private FileInStream openFile(TrinoInputFile file)
            throws IOException
    {
        try {
            return fileSystem.openFile(new AlluxioURI(file.location().toString()), OpenFilePOptions.getDefaultInstance());
        }
        catch (AlluxioException e) {
            throw new IOException("fail to open remote cache file", e);
        }
    }
}
