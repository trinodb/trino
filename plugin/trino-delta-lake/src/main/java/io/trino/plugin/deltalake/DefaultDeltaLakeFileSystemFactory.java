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
package io.trino.plugin.deltalake;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import io.trino.filesystem.manager.FileSystemConfig;
import io.trino.filesystem.manager.HdfsFileSystemLoader;
import io.trino.filesystem.memory.MemoryFileSystemCache;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.manager.FileSystemUtils.createDefaultFileSystemFactory;
import static java.util.Objects.requireNonNull;

public class DefaultDeltaLakeFileSystemFactory
        implements DeltaLakeFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public DefaultDeltaLakeFileSystemFactory(
            FileSystemConfig config,
            Optional<HdfsFileSystemLoader> hdfsFileSystemLoader,
            Map<String, TrinoFileSystemFactory> factories,
            Optional<TrinoFileSystemCache> fileSystemCache,
            Optional<MemoryFileSystemCache> memoryFileSystemCache,
            Optional<CacheKeyProvider> keyProvider,
            Tracer tracer)
    {
        this.fileSystemFactory = createDefaultFileSystemFactory(config, hdfsFileSystemLoader, factories, fileSystemCache, memoryFileSystemCache, keyProvider, tracer);
    }

    public DefaultDeltaLakeFileSystemFactory(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, VendedCredentialsHandle table)
    {
        return fileSystemFactory.create(session.getIdentity());
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session, String tableLocation)
    {
        return fileSystemFactory.create(session.getIdentity());
    }

    @Deprecated
    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return fileSystemFactory.create(identity);
    }
}
