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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.glue.v2.GlueHiveMetastoreConfig;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergSecurityConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSecurityConfig.IcebergSecurity.SYSTEM;
import static java.util.Objects.requireNonNull;

public class TrinoGlueCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final Optional<String> defaultSchemaLocation;
    private final GlueClient glueClient;
    private final boolean isUniqueTableLocation;
    private final boolean hideMaterializedViewStorageTable;
    private final boolean isUsingSystemSecurity;

    @Inject
    public TrinoGlueCatalogFactory(
            CatalogName catalogName,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            NodeVersion nodeVersion,
            GlueHiveMetastoreConfig glueConfig,
            IcebergConfig icebergConfig,
            IcebergGlueCatalogConfig catalogConfig,
            IcebergSecurityConfig securityConfig,
            GlueClient glueClient)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = catalogConfig.isCacheTableMetadata();
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = nodeVersion.toString();
        this.defaultSchemaLocation = glueConfig.getDefaultWarehouseDir();
        this.glueClient = requireNonNull(glueClient, "glueAsyncClient is null");
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.hideMaterializedViewStorageTable = icebergConfig.isHideMaterializedViewStorageTable();
        this.isUsingSystemSecurity = securityConfig.getSecuritySystem() == SYSTEM;
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoGlueCatalog(
                catalogName,
                fileSystemFactory,
                typeManager,
                cacheTableMetadata,
                tableOperationsProvider,
                trinoVersion,
                glueClient,
                isUsingSystemSecurity,
                defaultSchemaLocation,
                isUniqueTableLocation,
                hideMaterializedViewStorageTable);
    }
}
