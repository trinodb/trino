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

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.plugin.iceberg.ForIcebergMetadata;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.TRINO_CREATED_BY_VALUE;
import static java.util.Objects.requireNonNull;

public class TrinoHiveCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final HiveMetastoreFactory metastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final boolean isUniqueTableLocation;
    private final boolean isUsingSystemSecurity;
    private final boolean deleteSchemaLocationsFallback;
    private final boolean hideMaterializedViewStorageTable;
    private final Executor metadataFetchingExecutor;

    @Inject
    public TrinoHiveCatalogFactory(
            IcebergConfig config,
            CatalogName catalogName,
            HiveMetastoreFactory metastoreFactory,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            @UsingSystemSecurity boolean isUsingSystemSecurity,
            @ForIcebergMetadata ExecutorService metadataExecutorService)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationProvider is null");
        this.trinoVersion = nodeVersion.toString();
        this.isUniqueTableLocation = config.isUniqueTableLocation();
        this.isUsingSystemSecurity = isUsingSystemSecurity;
        this.deleteSchemaLocationsFallback = config.isDeleteSchemaLocationsFallback();
        this.hideMaterializedViewStorageTable = config.isHideMaterializedViewStorageTable();
        if (config.getMetadataParallelism() == 1) {
            this.metadataFetchingExecutor = directExecutor();
        }
        else {
            this.metadataFetchingExecutor = new BoundedExecutor(metadataExecutorService, config.getMetadataParallelism());
        }
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        CachingHiveMetastore metastore = createPerTransactionCache(metastoreFactory.createMetastore(Optional.of(identity)), 1000);
        return new TrinoHiveCatalog(
                catalogName,
                metastore,
                new TrinoViewHiveMetastore(metastore, isUsingSystemSecurity, trinoVersion, TRINO_CREATED_BY_VALUE),
                fileSystemFactory,
                typeManager,
                tableOperationsProvider,
                isUniqueTableLocation,
                isUsingSystemSecurity,
                deleteSchemaLocationsFallback,
                hideMaterializedViewStorageTable,
                metadataFetchingExecutor);
    }
}
