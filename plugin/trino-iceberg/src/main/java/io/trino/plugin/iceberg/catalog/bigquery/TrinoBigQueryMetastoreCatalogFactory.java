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
package io.trino.plugin.iceberg.catalog.bigquery;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.inject.Inject;
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreClientImpl;

import static java.util.Objects.requireNonNull;

public class TrinoBigQueryMetastoreCatalogFactory
        implements TrinoCatalogFactory
{
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final BigQueryMetastoreClientImpl bigQueryMetastoreClient;
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final String projectID;
    private final String gcpLocation;
    private final String listAllTables;
    private final String warehouse;
    private final boolean isUniqueTableLocation;

    @Inject
    public TrinoBigQueryMetastoreCatalogFactory(
            CatalogName catalogName,
            TypeManager typeManager,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            IcebergTableOperationsProvider tableOperationsProvider,
            BigQueryMetastoreClientImpl bigQueryMetastoreClient,
            IcebergBigQueryMetastoreCatalogConfig icebergBigQueryMetastoreCatalogConfig,
            IcebergConfig icebergConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.bigQueryMetastoreClient = requireNonNull(bigQueryMetastoreClient, "bigQueryMetastoreClient is null");
        this.gcpLocation = icebergBigQueryMetastoreCatalogConfig.getLocation();
        this.projectID = icebergBigQueryMetastoreCatalogConfig.getProjectID();
        this.listAllTables = icebergBigQueryMetastoreCatalogConfig.getListAllTables();
        this.warehouse = icebergBigQueryMetastoreCatalogConfig.getWarehouse();
        this.isUniqueTableLocation = icebergConfig.isUniqueTableLocation();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoBigQueryMetastoreCatalog(
                catalogName,
                typeManager,
                fileSystemFactory,
                fileIoFactory,
                tableOperationsProvider,
                bigQueryMetastoreClient,
                gcpLocation,
                projectID,
                listAllTables,
                warehouse,
                isUniqueTableLocation);
    }
}
