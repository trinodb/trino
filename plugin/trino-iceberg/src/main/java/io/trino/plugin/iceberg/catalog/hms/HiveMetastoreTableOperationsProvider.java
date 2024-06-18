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
package io.trino.plugin.iceberg.catalog.hms;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.CatalogType;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.io.FileIO;

import java.util.HashMap;
import java.util.Optional;

import static io.trino.plugin.iceberg.CatalogType.HIVE_METASTORE;
import static io.trino.plugin.iceberg.IcebergUtil.createFileIOCache;
import static io.trino.plugin.iceberg.IcebergUtil.loadManifestCachingProperties;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ThriftMetastoreFactory thriftMetastoreFactory;
    private final boolean isManifestCachingEnabled;
    private final DataSize maxManifestCacheSize;
    private final Duration manifestCacheExpireDuration;
    private final DataSize manifestCacheMaxContentLength;
    private final Optional<Cache<CatalogType, FileIO>> fileIOCache;

    @Inject
    public HiveMetastoreTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ThriftMetastoreFactory thriftMetastoreFactory,
            IcebergHiveMetastoreCatalogConfig icebergHiveMetastoreCatalogConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastoreFactory is null");
        requireNonNull(icebergHiveMetastoreCatalogConfig, "icebergHiveMetastoreCatalogConfig is null");
        this.isManifestCachingEnabled = icebergHiveMetastoreCatalogConfig.isManifestCachingEnabled();
        this.maxManifestCacheSize = icebergHiveMetastoreCatalogConfig.getMaxManifestCacheSize();
        this.manifestCacheExpireDuration = icebergHiveMetastoreCatalogConfig.getManifestCacheExpireDuration();
        this.manifestCacheMaxContentLength = icebergHiveMetastoreCatalogConfig.getManifestCacheMaxContentLength();
        this.fileIOCache = createFileIOCache(isManifestCachingEnabled, manifestCacheExpireDuration);
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        FileIO fileIO;
        if (fileIOCache.isPresent()) {
            fileIO = fileIOCache.get().get(
                    HIVE_METASTORE,
                    k -> new ForwardingFileIo(
                    fileSystemFactory.create(session),
                            loadManifestCachingProperties(
                                    new HashMap<>(),
                                    maxManifestCacheSize,
                                    manifestCacheExpireDuration,
                                    manifestCacheMaxContentLength)));
        }
        else {
            fileIO = new ForwardingFileIo(fileSystemFactory.create(session), ImmutableMap.of());
        }

        return new HiveMetastoreTableOperations(
                fileIO,
                ((TrinoHiveCatalog) catalog).getMetastore(),
                thriftMetastoreFactory.createMetastore(Optional.of(session.getIdentity())),
                session,
                database,
                table,
                owner,
                location);
    }
}
